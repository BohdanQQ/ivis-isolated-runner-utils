import selectors
import subprocess
import sys
import datetime
import requests
import json

class RequestFlushHandler:
    def __init__(self, emit_url_base: str, outputEventType: str) -> None:
        self.url = emit_url_base
        self.eventTypeVal = outputEventType

    def flush(self, output) -> None:
        msg = {
            'type': self.eventTypeVal,
            'data': output
        }
        requests.post(self.url, data=json.dumps(msg))

class OutputBuffer:
    def __init__(self, total_output_max_bytes, flush_timeout_secs, flusher, report_output_overflow) -> None:
        self.MAX_TOTAL_OUTPUT = total_output_max_bytes
        self.BUFF_FLUSH_TIMEOUT_SECS = flush_timeout_secs
        self.last_flush = None
        self.output_size = 0
        self.report_output_overflow = report_output_overflow
        self.output_has_overflown = False
        self.flusher = flusher
        self.output_buffer = []
        self.stderr_buffer = []

    def register_out(self, output):
        if self.output_size + len(output) > self.MAX_TOTAL_OUTPUT:
            if not self.output_has_overflown:
                self.output_has_overflown = True
                if self.report_output_overflow:
                    message = "INFO: max output capacity reached"
                    self.output_buffer.append(message)
                self.flush_buffer()
        else:
            self.output_size += len(output)
            self.output_buffer.append(output)
            self.try_flush_buffer()
    
    def register_stdout(self, output):
        self.register_out(output)

    def register_stderr(self, output):
        self.register_out(output)
        self.stderr_buffer.append(output)
    
    def buffer_flush_interval_elapsed(self):
        return self.last_flush is None or (datetime.datetime.now() - self.last_flush).total_seconds() >= self.BUFF_FLUSH_TIMEOUT_SECS

    def try_flush_buffer(self):
        if not self.output_has_overflown and self.buffer_flush_interval_elapsed():
            self.flush_buffer()
    
    def flush_buffer(self):
        self.last_flush = datetime.datetime.now()
        self.flusher.flush(self.output_buffer)
        self.output_buffer = []

def exitWithCode(code):
    print(str(code))
    exit(code)

if len(sys.argv) < 8:
    print("Invalid number of arguments")
    exitWithCode(1)

FILE_TO_EXECUTE=sys.argv[1]
BUFFER_MAX=int(sys.argv[2])
BUFFER_FLUSH_SECS=int(sys.argv[3])
EMIT_URL = sys.argv[4]
OUTPUT_EVENT_TYPE = sys.argv[5]


# -u to use unbuffered output
CMD = [sys.executable, '-u', FILE_TO_EXECUTE]

PROCESS = subprocess.Popen(
    CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
)

BUFFER = OutputBuffer(BUFFER_MAX, BUFFER_FLUSH_SECS, RequestFlushHandler(EMIT_URL, OUTPUT_EVENT_TYPE), True)

def after():
    process_status_code = PROCESS.wait()
    exitWithCode(process_status_code)


SELECTOR = selectors.DefaultSelector()
SELECTOR.register(PROCESS.stdout, selectors.EVENT_READ)
SELECTOR.register(PROCESS.stderr, selectors.EVENT_READ)
while True:
    for key, _ in SELECTOR.select():
        data = key.fileobj.read1().decode()
        if not data:
            after()
        if key.fileobj is PROCESS.stdout:
            BUFFER.register_stdout(data)
        else:
            BUFFER.register_stderr(data)