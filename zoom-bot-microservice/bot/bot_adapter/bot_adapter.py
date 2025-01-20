import zoom_meeting_sdk as zoom
from datetime import datetime
import numpy as np
import cv2
from .video_input_manager import VideoInputManager

import gi
gi.require_version('GLib', '2.0')
from gi.repository import GLib


def create_black_yuv420_frame(width=640, height=360):
    # Create BGR frame (red is [0,0,0] in BGR)
    bgr_frame = np.zeros((height, width, 3), dtype=np.uint8)
    bgr_frame[:, :] = [0, 0, 0]  # Pure black in BGR
    
    # Convert BGR to YUV420 (I420)
    yuv_frame = cv2.cvtColor(bgr_frame, cv2.COLOR_BGR2YUV_I420)
    
    # Return as bytes
    return yuv_frame.tobytes()


class ZoomBotAdapter:
    class Messages:
        LEAVE_MEETING_WAITING_FOR_HOST = "Leave meeting because received waiting for host status"
        ZOOM_AUTHORIZATION_FAILED = "Zoom authorization failed"
        BOT_PUT_IN_WAITING_ROOM = "Bot put in waiting room"
        BOT_JOINED_MEETING = "Bot joined meeting"
        BOT_RECORDING_PERMISSION_GRANTED = "Bot recording permission granted"
        MEETING_ENDED = "Meeting ended"
        NEW_UTTERANCE = "New utterance"

    def __init__(self, *, display_name, send_message_callback, add_audio_chunk_callback, meeting_password, token, meeting_id, add_video_frame_callback, wants_any_video_frames_callback, add_mixed_audio_chunk_callback):
        self.display_name = display_name
        self.send_message_callback = send_message_callback
        self.add_audio_chunk_callback = add_audio_chunk_callback
        self.add_mixed_audio_chunk_callback = add_mixed_audio_chunk_callback
        self.add_video_frame_callback = add_video_frame_callback
        self.wants_any_video_frames_callback = wants_any_video_frames_callback

        self._jwt_token = token
        self.meeting_id = meeting_id 
        self.meeting_password = meeting_password

        self.meeting_service = None
        self.setting_service = None
        self.auth_service = None

        self.auth_event = None
        self.recording_event = None
        self.meeting_service_event = None

        self.audio_source = None
        self.audio_helper = None

        self.audio_settings = None

        self.use_raw_recording = True
        self.recording_permission_granted = False

        self.reminder_controller = None

        self.recording_ctrl = None

        self.audio_raw_data_sender = None
        self.virtual_audio_mic_event_passthrough = None

        self.my_participant_id = None
        self.participants_ctrl = None
        self.meeting_reminder_event = None
        self.on_mic_start_send_callback_called = False
        self.on_virtual_camera_start_send_callback_called = False

        self.meeting_video_controller = None
        self.video_sender = None
        self.virtual_camera_video_source = None
        self.video_source_helper = None
        self.video_frame_size = (1920, 1080)

        self.video_input_manager = VideoInputManager(new_frame_callback=self.add_video_frame_callback, wants_any_frames_callback=self.wants_any_video_frames_callback, video_frame_size=self.video_frame_size)

        self.meeting_sharing_controller = None
        self.meeting_share_ctrl_event = None

        self.active_speaker_id = None
        self.active_sharer_id = None

        self._participant_cache = {}

    def on_user_join_callback(self, joined_user_ids, _):
        print("on_user_join_callback called. joined_user_ids =", joined_user_ids)
        for joined_user_id in joined_user_ids:
            self.get_participant(joined_user_id)

    def on_user_active_audio_change_callback(self, user_ids):
        if len(user_ids) == 0:
            return

        if user_ids[0] == self.my_participant_id:
            return

        if self.active_speaker_id == user_ids[0]:
            return

        self.active_speaker_id = user_ids[0]
        self.set_video_input_manager_based_on_state()

    def set_video_input_manager_based_on_state(self):
        if not self.wants_any_video_frames_callback():
            return
        
        if not self.recording_permission_granted:
            return
        
        print("set_video_input_manager_based_on_state self.active_sharer_id =", self.active_sharer_id, "self.active_speaker_id =", self.active_speaker_id)
        if self.active_sharer_id:
            self.video_input_manager.set_mode(mode=VideoInputManager.Mode.ACTIVE_SHARER, active_sharer_id=self.active_sharer_id, active_speaker_id=self.active_speaker_id)
        elif self.active_speaker_id:
            self.video_input_manager.set_mode(mode=VideoInputManager.Mode.ACTIVE_SPEAKER, active_sharer_id=self.active_sharer_id, active_speaker_id=self.active_speaker_id)
        else:
            # If there is no active sharer or speaker, we'll just use the video of the first participant that is not the bot
            # or if there are no participants, we'll use the bot
            default_participant_id = self.my_participant_id

            participant_list = self.participants_ctrl.GetParticipantsList()
            for participant_id in participant_list:
                if participant_id != self.my_participant_id:
                    default_participant_id = participant_id
                    break

            print("set_video_input_manager_based_on_state hit default case. default_participant_id =", default_participant_id)
            self.video_input_manager.set_mode(mode=VideoInputManager.Mode.ACTIVE_SPEAKER, active_speaker_id=default_participant_id, active_sharer_id=None)
            
    def set_up_video_input_manager(self):
        # If someone was sharing before we joined, we will not receive an event, so we need to poll for the active sharer
        viewable_share_source_list = self.meeting_sharing_controller.GetViewableShareSourceList()
        self.active_sharer_id = viewable_share_source_list[0] if viewable_share_source_list else None

        self.set_video_input_manager_based_on_state()

    def cleanup(self):
        if self.audio_source:
            performance_data = self.audio_source.getPerformanceData()
            print("totalProcessingTimeMicroseconds =", performance_data.totalProcessingTimeMicroseconds)
            print("numCalls =", performance_data.numCalls)
            print("maxProcessingTimeMicroseconds =", performance_data.maxProcessingTimeMicroseconds)
            print("minProcessingTimeMicroseconds =", performance_data.minProcessingTimeMicroseconds)
            print("meanProcessingTimeMicroseconds =", float(performance_data.totalProcessingTimeMicroseconds) / performance_data.numCalls)

            # Print processing time distribution
            bin_size = (performance_data.processingTimeBinMax - performance_data.processingTimeBinMin) / len(performance_data.processingTimeBinCounts)
            print("\nProcessing time distribution (microseconds):")
            for bin_idx, count in enumerate(performance_data.processingTimeBinCounts):
                if count > 0:
                    bin_start = bin_idx * bin_size
                    bin_end = (bin_idx + 1) * bin_size
                    print(f"{bin_start:6.0f} - {bin_end:6.0f} us: {count:5d} calls")

        if self.meeting_service:
            zoom.DestroyMeetingService(self.meeting_service)
            print("Destroyed Meeting service")
        if self.setting_service:
            zoom.DestroySettingService(self.setting_service)
            print("Destroyed Setting service")
        if self.auth_service:
            zoom.DestroyAuthService(self.auth_service)
            print("Destroyed Auth service")

        if self.audio_helper:
            audio_helper_unsubscribe_result = self.audio_helper.unSubscribe()
            print("audio_helper.unSubscribe() returned", audio_helper_unsubscribe_result)

        if self.video_input_manager:
            self.video_input_manager.cleanup()

        print("CleanUPSDK() called")
        zoom.CleanUPSDK()
        print("CleanUPSDK() finished")

    def init(self):
        init_param = zoom.InitParam()

        init_param.strWebDomain = "https://zoom.us"
        init_param.strSupportUrl = "https://zoom.us"
        init_param.enableGenerateDump = True
        init_param.emLanguageID = zoom.SDK_LANGUAGE_ID.LANGUAGE_English
        init_param.enableLogByDefault = True

        init_sdk_result = zoom.InitSDK(init_param)
        if init_sdk_result != zoom.SDKERR_SUCCESS:
            raise Exception('InitSDK failed')
        
        self.create_services()

    def get_participant(self, participant_id):
        try:
            speaker_object = self.participants_ctrl.GetUserByUserID(participant_id)
            participant_info = {
                'participant_uuid': participant_id,
                'participant_user_uuid': speaker_object.GetPersistentId(),
                'participant_full_name': speaker_object.GetUserName()
            }
            self._participant_cache[participant_id] = participant_info
            return participant_info
        except:
            print(f"Error getting participant {participant_id}, falling back to cache")
            return self._participant_cache.get(participant_id)

    def on_sharing_status_callback(self, sharing_status, user_id):
        print("on_sharing_status_callback called. sharing_status =", sharing_status, "user_id =", user_id)

        if sharing_status == zoom.Sharing_Other_Share_Begin or sharing_status == zoom.Sharing_View_Other_Sharing:
            new_active_sharer_id = user_id
        else:
            new_active_sharer_id = None

        if new_active_sharer_id != self.active_sharer_id:
            self.active_sharer_id = new_active_sharer_id
            self.set_video_input_manager_based_on_state()

    def on_join(self):
        # Meeting reminder controller
        self.meeting_reminder_event = zoom.MeetingReminderEventCallbacks(onReminderNotifyCallback=self.on_reminder_notify)
        self.reminder_controller = self.meeting_service.GetMeetingReminderController()
        self.reminder_controller.SetEvent(self.meeting_reminder_event)

        # Participants controller
        self.participants_ctrl = self.meeting_service.GetMeetingParticipantsController()
        self.participants_ctrl_event = zoom.MeetingParticipantsCtrlEventCallbacks(onUserJoinCallback=self.on_user_join_callback)
        self.participants_ctrl.SetEvent(self.participants_ctrl_event)
        self.my_participant_id = self.participants_ctrl.GetMySelfUser().GetUserID()
        participant_ids_list = self.participants_ctrl.GetParticipantsList()
        for participant_id in participant_ids_list:
            self.get_participant(participant_id)

        # Meeting sharing controller
        self.meeting_sharing_controller = self.meeting_service.GetMeetingShareController()
        self.meeting_share_ctrl_event = zoom.MeetingShareCtrlEventCallbacks(onSharingStatusCallback=self.on_sharing_status_callback)
        self.meeting_sharing_controller.SetEvent(self.meeting_share_ctrl_event)

        # Audio controller
        self.audio_ctrl = self.meeting_service.GetMeetingAudioController()
        self.audio_ctrl_event = zoom.MeetingAudioCtrlEventCallbacks(onUserActiveAudioChangeCallback=self.on_user_active_audio_change_callback)
        self.audio_ctrl.SetEvent(self.audio_ctrl_event)

        if self.use_raw_recording:
            self.recording_ctrl = self.meeting_service.GetMeetingRecordingController()

            def on_recording_privilege_changed(can_rec):
                print("on_recording_privilege_changed called. can_record =", can_rec)
                if can_rec:
                    self.start_raw_recording()
                else:
                    self.stop_raw_recording()

            self.recording_event = zoom.MeetingRecordingCtrlEventCallbacks(onRecordPrivilegeChangedCallback=on_recording_privilege_changed)
            self.recording_ctrl.SetEvent(self.recording_event)

            self.start_raw_recording()

        # Set up media streams
        GLib.timeout_add_seconds(1, self.set_up_bot_audio_input)
        GLib.timeout_add_seconds(1, self.set_up_bot_video_input)

    def set_up_bot_video_input(self):
        self.virtual_camera_video_source = zoom.ZoomSDKVideoSourceCallbacks(onInitializeCallback=self.on_virtual_camera_initialize_callback, onStartSendCallback=self.on_virtual_camera_start_send_callback)
        self.video_source_helper = zoom.GetRawdataVideoSourceHelper()
        if self.video_source_helper:
            set_external_video_source_result = self.video_source_helper.setExternalVideoSource(self.virtual_camera_video_source)
            print("set_external_video_source_result =", set_external_video_source_result)
            if set_external_video_source_result == zoom.SDKERR_SUCCESS:
                self.meeting_video_controller = self.meeting_service.GetMeetingVideoController()
                unmute_video_result = self.meeting_video_controller.UnmuteVideo()
                print("unmute_video_result =", unmute_video_result)
        else:
            print("video_source_helper is None")

    def on_virtual_camera_start_send_callback(self):
        print("on_virtual_camera_start_send_callback called")
        # As soon as we get this callback, we need to send a blank frame and it will fail with SDKERR_WRONG_USAGE
        # Then the callback will be triggered again and subsequent calls will succeed.
        # Not sure why this happens.
        if self.video_sender and not self.on_virtual_camera_start_send_callback_called:
            blank = create_black_yuv420_frame(640, 360)
            initial_send_video_frame_response = self.video_sender.sendVideoFrame(blank, 640, 360, 0, zoom.FrameDataFormat_I420_FULL)
            print("initial_send_video_frame_response =", initial_send_video_frame_response)
        self.on_virtual_camera_start_send_callback_called = True

    def on_virtual_camera_initialize_callback(self, video_sender, support_cap_list, suggest_cap):
        self.video_sender = video_sender

    def send_raw_image(self, yuv420_image_bytes):
        if not self.on_virtual_camera_start_send_callback_called:
            raise Exception("on_virtual_camera_start_send_callback_called not called so cannot send raw image")
        send_video_frame_response = self.video_sender.sendVideoFrame(yuv420_image_bytes, 640, 360, 0, zoom.FrameDataFormat_I420_FULL)
        print("send_raw_image send_video_frame_response =", send_video_frame_response)

    def set_up_bot_audio_input(self):
        if self.audio_helper is None:
            self.audio_helper = zoom.GetAudioRawdataHelper()

        if self.audio_helper is None:
            print("set_up_bot_audio_input failed because audio_helper is None")
            return

        self.virtual_audio_mic_event_passthrough = zoom.ZoomSDKVirtualAudioMicEventCallbacks(
            onMicInitializeCallback=self.on_mic_initialize_callback, 
            onMicStartSendCallback=self.on_mic_start_send_callback
        )

        audio_helper_set_external_audio_source_result = self.audio_helper.setExternalAudioSource(self.virtual_audio_mic_event_passthrough)
        print("audio_helper_set_external_audio_source_result =", audio_helper_set_external_audio_source_result)
        if audio_helper_set_external_audio_source_result != zoom.SDKERR_SUCCESS:
            print("Failed to set external audio source")
            return

    def on_mic_initialize_callback(self, sender):
        self.audio_raw_data_sender = sender

    def send_raw_audio(self, bytes):
        if not self.on_mic_start_send_callback_called:
            raise Exception("on_mic_start_send_callback_called not called so cannot send raw audio")
        self.audio_raw_data_sender.send(bytes, 8000, zoom.ZoomSDKAudioChannel_Mono)

    def on_mic_start_send_callback(self):
        self.on_mic_start_send_callback_called = True
        print("on_mic_start_send_callback called")

    def on_one_way_audio_raw_data_received_callback(self, data, node_id):
        if node_id == self.my_participant_id:
            return
        
        current_time = datetime.utcnow()

        self.add_audio_chunk_callback(node_id, current_time, data.GetBuffer())

    def start_raw_recording(self):
        self.recording_ctrl = self.meeting_service.GetMeetingRecordingController()

        can_start_recording_result = self.recording_ctrl.CanStartRawRecording()
        if can_start_recording_result != zoom.SDKERR_SUCCESS:
            self.recording_ctrl.RequestLocalRecordingPrivilege()
            print("Requesting recording privilege.")
            return

        start_raw_recording_result = self.recording_ctrl.StartRawRecording()
        if start_raw_recording_result != zoom.SDKERR_SUCCESS:
            print("Start raw recording failed.")
            return

        if self.audio_helper is None:
            self.audio_helper = zoom.GetAudioRawdataHelper()
        if self.audio_helper is None:
            print("audio_helper is None")
            return
        
        if self.audio_source is None:
            self.audio_source = zoom.ZoomSDKAudioRawDataDelegateCallbacks(
                collectPerformanceData=True, 
                onOneWayAudioRawDataReceivedCallback=self.on_one_way_audio_raw_data_received_callback,
                onMixedAudioRawDataReceivedCallback=self.add_mixed_audio_chunk_callback
            )

        audio_helper_subscribe_result = self.audio_helper.subscribe(self.audio_source, False)
        print("audio_helper_subscribe_result =",audio_helper_subscribe_result)

        self.send_message_callback({'message': self.Messages.BOT_RECORDING_PERMISSION_GRANTED})
        self.recording_permission_granted = True

        GLib.timeout_add(100, self.set_up_video_input_manager)

    def stop_raw_recording(self):
        rec_ctrl = self.meeting_service.StopRawRecording()
        if rec_ctrl.StopRawRecording() != zoom.SDKERR_SUCCESS:
            raise Exception("Error with stop raw recording")

    def leave(self):
        if self.meeting_service is None:
            return
        
        status = self.meeting_service.GetMeetingStatus()
        if status == zoom.MEETING_STATUS_IDLE or status == zoom.MEETING_STATUS_ENDED:
            print("Aborting leave because meeting status is", status)
            return

        print("Leaving meeting...")
        leave_result = self.meeting_service.Leave(zoom.LEAVE_MEETING)
        print("Left meeting. result =", leave_result)


    def join_meeting(self):
        meeting_number = int(self.meeting_id)

        join_param = zoom.JoinParam()
        join_param.userType = zoom.SDKUserType.SDK_UT_WITHOUT_LOGIN

        param = join_param.param
        param.meetingNumber = meeting_number
        param.userName = self.display_name
        param.psw = self.meeting_password if self.meeting_password is not None else ""
        param.vanityID = ""
        param.customer_key = ""
        param.webinarToken = ""
        param.isVideoOff = False
        param.isAudioOff = False

        join_result = self.meeting_service.Join(join_param)
        print("join_result =",join_result)

        self.audio_settings = self.setting_service.GetAudioSettings()
        self.audio_settings.EnableAutoJoinAudio(True)

    def on_reminder_notify(self, content, handler):
        if handler:
            handler.Accept()

    def auth_return(self, result):
        if result == zoom.AUTHRET_SUCCESS:
            print("Auth completed successfully.")
            return self.join_meeting()

        self.send_message_callback({'message': self.Messages.ZOOM_AUTHORIZATION_FAILED, 'zoom_result_code': result})
    
    def meeting_status_changed(self, status, iResult):
        print("meeting_status_changed called. status =",status,"iResult=",iResult)

        if status == zoom.MEETING_STATUS_WAITINGFORHOST:
            self.send_message_callback({'message': self.Messages.LEAVE_MEETING_WAITING_FOR_HOST})

        if status == zoom.MEETING_STATUS_IN_WAITING_ROOM:
            self.send_message_callback({'message': self.Messages.BOT_PUT_IN_WAITING_ROOM})

        if status == zoom.MEETING_STATUS_INMEETING:
            self.send_message_callback({'message': self.Messages.BOT_JOINED_MEETING})

        if status == zoom.MEETING_STATUS_ENDED:
            self.send_message_callback({'message': self.Messages.MEETING_ENDED})


        if status == zoom.MEETING_STATUS_INMEETING:
            return self.on_join()
        

    def create_services(self):
        self.meeting_service = zoom.CreateMeetingService()
        
        self.setting_service = zoom.CreateSettingService()

        self.meeting_service_event = zoom.MeetingServiceEventCallbacks(onMeetingStatusChangedCallback=self.meeting_status_changed)
                
        meeting_service_set_revent_result = self.meeting_service.SetEvent(self.meeting_service_event)
        if meeting_service_set_revent_result != zoom.SDKERR_SUCCESS:
            raise Exception("Meeting Service set event failed")
        
        self.auth_event = zoom.AuthServiceEventCallbacks(onAuthenticationReturnCallback=self.auth_return)

        self.auth_service = zoom.CreateAuthService()

        set_event_result = self.auth_service.SetEvent(self.auth_event)
        print("set_event_result =",set_event_result)
    
        # Use the auth service
        auth_context = zoom.AuthContext()
        auth_context.jwt_token = self._jwt_token

        result = self.auth_service.SDKAuth(auth_context)
    
        if result == zoom.SDKError.SDKERR_SUCCESS:
            print("Authentication successful")
        else:
            print("Authentication failed with error:", result)