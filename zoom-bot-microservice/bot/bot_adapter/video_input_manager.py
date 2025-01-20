import zoom_meeting_sdk as zoom
import numpy as np
import cv2
from gi.repository import GLib
import time
import logging

logger = logging.getLogger(__name__)

def create_black_i420_frame(video_frame_size):
    width, height = video_frame_size
    # Ensure dimensions are even for proper chroma subsampling
    if width % 2 != 0 or height % 2 != 0:
        raise ValueError("Width and height must be even numbers for I420 format")
    
    # Y plane (black = 0 in Y plane)
    y_plane = np.zeros((height, width), dtype=np.uint8)
    
    # U and V planes (black = 128 in UV planes)
    # Both are quarter size of original due to 4:2:0 subsampling
    u_plane = np.full((height // 2, width // 2), 128, dtype=np.uint8)
    v_plane = np.full((height // 2, width // 2), 128, dtype=np.uint8)
    
    # Concatenate all planes
    yuv_frame = np.concatenate([
        y_plane.flatten(),
        u_plane.flatten(),
        v_plane.flatten()
    ])
    
    return yuv_frame.astype(np.uint8).tobytes()

def scale_i420(frame, new_size):
    new_width, new_height = new_size
    """
    Scales the given frame in I420 format to new_width x new_height while
    preserving aspect ratio. If the aspect ratios do not match, letterboxes/pillarboxes
    the scaled image on a black background.
    
    :param frame: Frame object with methods:
        - GetStreamWidth()
        - GetStreamHeight()
        - GetYBuffer()
        - GetUBuffer()
        - GetVBuffer()
    :param new_width: Desired width.
    :param new_height: Desired height.
    :return: Scaled (and possibly letter/pillarboxed) I420 frame bytes.
    """
    orig_width = frame.GetStreamWidth()
    orig_height = frame.GetStreamHeight()

    # 1) Convert buffers to NumPy arrays without extra copies if possible.
    y = np.frombuffer(frame.GetYBuffer(), dtype=np.uint8, count=orig_width*orig_height)
    u = np.frombuffer(frame.GetUBuffer(), dtype=np.uint8, count=(orig_width//2)*(orig_height//2))
    v = np.frombuffer(frame.GetVBuffer(), dtype=np.uint8, count=(orig_width//2)*(orig_height//2))

    # Reshape planes
    y = y.reshape(orig_height, orig_width)
    u = u.reshape(orig_height//2, orig_width//2)
    v = v.reshape(orig_height//2, orig_width//2)

    # 2) Determine scale preserving aspect ratio
    input_aspect = orig_width / orig_height
    output_aspect = new_width / new_height

    if abs(input_aspect - output_aspect) < 1e-6:
        # Aspect ratios match (or extremely close). Just do a simple stretch to (new_width, new_height).
        scaled_y = cv2.resize(y, (new_width, new_height), interpolation=cv2.INTER_LINEAR)
        scaled_u = cv2.resize(u, (new_width//2, new_height//2), interpolation=cv2.INTER_LINEAR)
        scaled_v = cv2.resize(v, (new_width//2, new_height//2), interpolation=cv2.INTER_LINEAR)

        # Flatten and return
        return np.concatenate([
            scaled_y.flatten(),
            scaled_u.flatten(),
            scaled_v.flatten()
        ]).astype(np.uint8).tobytes()

    # Otherwise, the aspect ratios differ => letterbox or pillarbox
    # 3) Compute scaled dimensions that fit entirely within (new_width, new_height)
    if input_aspect > output_aspect:
        # The image is relatively wider => match width, shrink height
        scaled_width = new_width
        scaled_height = int(round(new_width / input_aspect))
    else:
        # The image is relatively taller => match height, shrink width
        scaled_height = new_height
        scaled_width = int(round(new_height * input_aspect))

    # 4) Resize Y, U, and V to the scaled dimensions
    scaled_y = cv2.resize(y, (scaled_width, scaled_height), interpolation=cv2.INTER_LINEAR)
    scaled_u = cv2.resize(u, (scaled_width//2, scaled_height//2), interpolation=cv2.INTER_LINEAR)
    scaled_v = cv2.resize(v, (scaled_width//2, scaled_height//2), interpolation=cv2.INTER_LINEAR)

    # 5) Create the black background only if needed
    # For I420, black is typically (Y=0, U=128, V=128) or (Y=16, U=128, V=128).
    # We'll use Y=0, U=128, V=128 for "dark" black.
    final_y = np.zeros((new_height, new_width), dtype=np.uint8)
    final_u = np.full((new_height//2, new_width//2), 128, dtype=np.uint8)
    final_v = np.full((new_height//2, new_width//2), 128, dtype=np.uint8)

    # 6) Compute centering offsets for each plane
    # For Y-plane
    offset_y = (new_height - scaled_height) // 2
    offset_x = (new_width - scaled_width) // 2

    # Insert Y
    final_y[offset_y:offset_y+scaled_height, offset_x:offset_x+scaled_width] = scaled_y

    # For U, V planes (subsampled by 2 in each dimension)
    offset_y_uv = offset_y // 2
    offset_x_uv = offset_x // 2

    final_u[offset_y_uv:offset_y_uv+(scaled_height//2),
            offset_x_uv:offset_x_uv+(scaled_width//2)] = scaled_u
    final_v[offset_y_uv:offset_y_uv+(scaled_height//2),
            offset_x_uv:offset_x_uv+(scaled_width//2)] = scaled_v

    # 7) Flatten back to I420 layout and return bytes
    return np.concatenate([
        final_y.flatten(),
        final_u.flatten(),
        final_v.flatten()
    ]).astype(np.uint8).tobytes()

class VideoInputStream:
    def __init__(self, video_input_manager, user_id, stream_type):
        self.video_input_manager = video_input_manager
        self.user_id = user_id
        self.stream_type = stream_type
        self.renderer_destroyed = False
        self.renderer_delegate = zoom.ZoomSDKRendererDelegateCallbacks(
            onRawDataFrameReceivedCallback=self.on_raw_video_frame_received_callback,
            onRendererBeDestroyedCallback=self.on_renderer_destroyed_callback,
            onRawDataStatusChangedCallback=self.on_raw_data_status_changed_callback
        )

        self.renderer = zoom.createRenderer(self.renderer_delegate)
        set_resolution_result = self.renderer.setRawDataResolution(zoom.ZoomSDKResolution_180P)
        raw_data_type = {
            VideoInputManager.StreamType.SCREENSHARE: zoom.ZoomSDKRawDataType.RAW_DATA_TYPE_SHARE,
            VideoInputManager.StreamType.VIDEO: zoom.ZoomSDKRawDataType.RAW_DATA_TYPE_VIDEO
        }[stream_type]
        
        subscribe_result = self.renderer.subscribe(self.user_id, raw_data_type)
        self.raw_data_status = zoom.RawData_Off

        self.last_frame_time = time.time()
        self.black_frame_timer_id = GLib.timeout_add(250, self.send_black_frame)

        logger.info(f"In VideoInputStream.init self.renderer = {self.renderer}")
        logger.info(f"In VideoInputStream.init set_resolution_result for user {self.user_id} is {set_resolution_result}")
        logger.info(f"In VideoInputStream.init subscribe_result for user {self.user_id} is {subscribe_result}")
        self.last_debug_frame_time = None

    def on_raw_data_status_changed_callback(self, status):
        self.raw_data_status = status
        logger.info(f"In VideoInputStream.on_raw_data_status_changed_callback raw_data_status for user {self.user_id} is {self.raw_data_status}")

    def send_black_frame(self):
        if self.renderer_destroyed:
            return False
            
        current_time = time.time()
        if current_time - self.last_frame_time >= 0.25 and self.raw_data_status == zoom.RawData_Off:
            # Create a black frame of the same dimensions
            black_frame = create_black_i420_frame(self.video_input_manager.video_frame_size)
            self.video_input_manager.new_frame_callback(black_frame, time.time_ns())
            logger.info(f"In VideoInputStream.send_black_frame for user {self.user_id} sent black frame")
            
        return not self.renderer_destroyed  # Continue timer if not cleaned up

    def cleanup(self):
        if self.renderer_destroyed:
            return
        
        if self.black_frame_timer_id is not None:
            GLib.source_remove(self.black_frame_timer_id)
            self.black_frame_timer_id = None

        logger.info(f"starting renderer unsubscription for user {self.user_id}")
        self.renderer.unSubscribe()
        logger.info(f"finished renderer unsubscription for user {self.user_id}")

    def on_renderer_destroyed_callback(self):
        self.renderer_destroyed = True
        logger.info(f"renderer destroyed for user {self.user_id}")

    def on_raw_video_frame_received_callback(self, data):
        current_time_ns = time.time_ns()

        if self.renderer_destroyed:
            return
        
        if not self.video_input_manager.wants_frames_for_user(self.user_id):
            return
        
        self.last_frame_time = time.time()

        i420_frame = data.GetBuffer()

        if i420_frame is None or len(i420_frame) == 0:
            logger.warning(f"In VideoInputStream.on_raw_video_frame_received_callback invalid frame received for user {self.user_id}")
            return

        if self.last_debug_frame_time is None or time.time() - self.last_debug_frame_time > 1:
            logger.info(f"In VideoInputStream.on_raw_video_frame_received_callback for user {self.user_id} received frame")
            self.last_debug_frame_time = time.time()

        scaled_i420_frame = scale_i420(data, self.video_input_manager.video_frame_size)
        self.video_input_manager.new_frame_callback(scaled_i420_frame, current_time_ns)

class VideoInputManager:
    class StreamType:
        VIDEO = 1
        SCREENSHARE = 2

    class Mode:
        ACTIVE_SPEAKER = 1
        ACTIVE_SHARER = 2

    def __init__(self, *, new_frame_callback, wants_any_frames_callback, video_frame_size):
        self.new_frame_callback = new_frame_callback
        self.wants_any_frames_callback = wants_any_frames_callback
        self.video_frame_size = video_frame_size
        self.mode = None
        self.input_streams = []

    def has_any_video_input_streams(self):
        return len(self.input_streams) > 0

    def add_input_streams_if_needed(self, streams_info):
        streams_to_remove = [
            input_stream for input_stream in self.input_streams 
            if not any(
                stream_info['user_id'] == input_stream.user_id and 
                stream_info['stream_type'] == input_stream.stream_type 
                for stream_info in streams_info
            )
        ]

        for stream in streams_to_remove:
            stream.cleanup()
            self.input_streams.remove(stream)

        for stream_info in streams_info:
            if any(input_stream.user_id == stream_info['user_id'] and input_stream.stream_type == stream_info['stream_type'] for input_stream in self.input_streams):
                continue

            self.input_streams.append(VideoInputStream(self, stream_info['user_id'], stream_info['stream_type']))

    def cleanup(self):
        for input_stream in self.input_streams:
            input_stream.cleanup()

    def set_mode(self, *, mode, active_speaker_id, active_sharer_id):
        if mode != VideoInputManager.Mode.ACTIVE_SPEAKER and mode != VideoInputManager.Mode.ACTIVE_SHARER:
            raise Exception("Unsupported mode " + str(mode))
        
        print(f"In VideoInputManager.set_mode mode = {mode} active_speaker_id = {active_speaker_id} active_sharer_id = {active_sharer_id}")

        self.mode = mode

        if self.mode == VideoInputManager.Mode.ACTIVE_SPEAKER:
            self.active_speaker_id = active_speaker_id
            self.add_input_streams_if_needed([{"stream_type": VideoInputManager.StreamType.VIDEO, "user_id": active_speaker_id}])

        if self.mode == VideoInputManager.Mode.ACTIVE_SHARER:
            self.active_sharer_id = active_sharer_id
            self.add_input_streams_if_needed([{"stream_type": VideoInputManager.StreamType.SCREENSHARE, "user_id": active_sharer_id}])

    def wants_frames_for_user(self, user_id):
        if not self.wants_any_frames_callback():
            return False
    
        if self.mode == VideoInputManager.Mode.ACTIVE_SPEAKER and user_id != self.active_speaker_id:
            return False

        if self.mode == VideoInputManager.Mode.ACTIVE_SHARER and user_id != self.active_sharer_id:
            return False

        return True