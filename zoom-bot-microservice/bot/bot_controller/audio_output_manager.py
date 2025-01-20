import time

class AudioOutputManager:
    def __init__(self, currently_playing_audio_media_request_finished_callback):
        self.currently_playing_audio_media_request = None
        self.currently_playing_audio_media_request_started_at = None
        self.currently_playing_audio_media_request_finished_callback = currently_playing_audio_media_request_finished_callback

    def start_playing_audio_media_request(self, audio_media_request):
        self.currently_playing_audio_media_request = audio_media_request
        self.currently_playing_audio_media_request_started_at = time.time()

    def currently_playing_audio_media_request_is_finished(self):
        if not self.currently_playing_audio_media_request or not self.currently_playing_audio_media_request_started_at:
            return False
        elapsed_ms = (time.time() - self.currently_playing_audio_media_request_started_at) * 1000
        if elapsed_ms > self.currently_playing_audio_media_request.duration_ms:
            return True
        return False
    
    def clear_currently_playing_audio_media_request(self):
        self.currently_playing_audio_media_request = None
        self.currently_playing_audio_media_request_started_at = None

    def monitor_currently_playing_audio_media_request(self):
        if self.currently_playing_audio_media_request_is_finished():
            temp_currently_playing_audio_media_request = self.currently_playing_audio_media_request
            self.clear_currently_playing_audio_media_request()
            self.currently_playing_audio_media_request_finished_callback(temp_currently_playing_audio_media_request)