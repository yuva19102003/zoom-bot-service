from pydub import AudioSegment
import io
import cv2
import numpy as np

def pcm_to_mp3(pcm_data: bytes, sample_rate: int = 32000, channels: int = 1, sample_width: int = 2, bitrate: str = "128k") -> bytes:
    """
    Convert PCM audio data to MP3 format.
    
    Args:
        pcm_data (bytes): Raw PCM audio data
        sample_rate (int): Sample rate in Hz (default: 32000)
        channels (int): Number of audio channels (default: 1)
        sample_width (int): Sample width in bytes (default: 2)
        bitrate (str): MP3 encoding bitrate (default: "128k")
    
    Returns:
        bytes: MP3 encoded audio data
    """
    # Create AudioSegment from raw PCM data
    audio_segment = AudioSegment(
        data=pcm_data,
        sample_width=sample_width,
        frame_rate=sample_rate,
        channels=channels
    )

    # Create a bytes buffer to store the MP3 data
    buffer = io.BytesIO()
    
    # Export the audio segment as MP3 to the buffer with specified bitrate
    audio_segment.export(buffer, format='mp3', parameters=["-b:a", bitrate])
    
    # Get the MP3 data as bytes
    mp3_data = buffer.getvalue()
    buffer.close()
    
    return mp3_data

def mp3_to_pcm(mp3_data: bytes, sample_rate: int = 32000, channels: int = 1, sample_width: int = 2) -> bytes:
    """
    Convert MP3 audio data to PCM format.
    
    Args:
        mp3_data (bytes): MP3 audio data
        sample_rate (int): Desired sample rate in Hz (default: 32000)
        channels (int): Desired number of audio channels (default: 1)
        sample_width (int): Desired sample width in bytes (default: 2)
    
    Returns:
        bytes: Raw PCM audio data
    """
    # Create a bytes buffer from the MP3 data
    buffer = io.BytesIO(mp3_data)
    
    # Load the MP3 data into an AudioSegment
    audio_segment = AudioSegment.from_mp3(buffer)
    
    # Convert to the desired format
    audio_segment = audio_segment.set_frame_rate(sample_rate)
    audio_segment = audio_segment.set_channels(channels)
    audio_segment = audio_segment.set_sample_width(sample_width)
    
    # Get the raw PCM data
    pcm_data = audio_segment.raw_data
    buffer.close()
    
    return pcm_data

def calculate_audio_duration_ms(audio_data: bytes, content_type: str) -> int:
    """
    Calculate the duration of audio data in milliseconds.
    
    Args:
        audio_data (bytes): Audio data in either PCM or MP3 format
        content_type (str): Content type of the audio data (e.g., 'audio/mp3')
    
    Returns:
        int: Duration in milliseconds
    """
    buffer = io.BytesIO(audio_data)
    
    if content_type == 'audio/mp3':
        audio = AudioSegment.from_mp3(buffer)
    else:
        raise ValueError(f"Unsupported content type for duration calculation: {content_type}")
    
    buffer.close()
    # len(audio) returns duration in milliseconds for pydub AudioSegment objects
    duration_ms = len(audio)
    return duration_ms

def png_to_yuv420_frame(png_bytes: bytes, width: int = 640, height: int = 360) -> bytes:
    """
    Convert PNG image bytes to YUV420 (I420) format and resize to specified dimensions.
    
    Args:
        png_bytes (bytes): Input PNG image as bytes
        width (int): Desired width of output frame (default: 640)
        height (int): Desired height of output frame (default: 360)
    
    Returns:
        bytes: YUV420 formatted frame data
    """
    # Convert PNG bytes to numpy array
    png_array = np.frombuffer(png_bytes, np.uint8)
    bgr_frame = cv2.imdecode(png_array, cv2.IMREAD_COLOR)
    
    # Resize the frame to desired dimensions
    bgr_frame = cv2.resize(bgr_frame, (width, height), interpolation=cv2.INTER_AREA)
    
    # Convert BGR to YUV420 (I420)
    yuv_frame = cv2.cvtColor(bgr_frame, cv2.COLOR_BGR2YUV_I420)
    
    # Return as bytes
    return yuv_frame.tobytes()
