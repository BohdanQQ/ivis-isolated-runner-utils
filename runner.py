import selectors
import subprocess
import sys
import datetime
import requests
import time

class RequestFlushHandler:
    def __init__(self, emit_url_base: str, output_event_type: str, cert_paths) -> None:
        self.url = emit_url_base
        self.event_type_val = output_event_type
        self.cert_paths = cert_paths

    def flush(self, output) -> None:
        requests.post(self.url, json={
            'type': self.event_type_val,
            'data': "".join(output)
        }, cert=self.cert_paths)

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
        self.stdout = ""
        self.stderr = ""

    def register_out(self, output):
        if self.output_size + len(output) > self.MAX_TOTAL_OUTPUT:
            if not self.output_has_overflown:
                self.output_has_overflown = True
                if self.report_output_overflow:
                    message = "INFO: max output capacity reached"
                    self.stdout += message + '\n'
                    self.output_buffer.append(message)
                self.flush_buffer()
        else:
            self.output_size += len(output)
            self.output_buffer.append(output)
            self.try_flush_buffer()
    
    def register_stdout(self, output):
        self.register_out(output)
        self.stdout += output

    def register_stderr(self, output):
        # according to IVIS-core implementation, stderr does not count against output bytes
        self.stderr += output
    
    def buffer_flush_interval_elapsed(self):
        return self.last_flush is None or (datetime.datetime.now() - self.last_flush).total_seconds() >= self.BUFF_FLUSH_TIMEOUT_SECS

    def try_flush_buffer(self):
        if not self.output_has_overflown and self.buffer_flush_interval_elapsed():
            self.flush_buffer()
    
    def flush_buffer(self):
        self.last_flush = datetime.datetime.now()
        if len(self.output_buffer) == 0:
            return
        self.flusher.flush(self.output_buffer)
        self.output_buffer = []

def exit_with_code(code):
    print(str(code))
    exit(code)

if len(sys.argv) < 15:
    print("Invalid number of arguments")
    exit_with_code(1)

FILE_TO_EXECUTE=sys.argv[1]
BUFFER_MAX=int(sys.argv[2])
BUFFER_FLUSH_SECS=int(sys.argv[3])
EMIT_URL = sys.argv[4]
OUTPUT_EVENT_TYPE = sys.argv[5]
FAIL_EVENT_TYPE = sys.argv[6]
SUCCESS_EVENT_TYPE = sys.argv[7]
STATUS_URL = sys.argv[8]
FAIL_STATUS_CODE = sys.argv[9]
SUCCESS_STATUS_CODE = sys.argv[10]
CERT_PATH = sys.argv[11]
KEY_PATH = sys.argv[12]
RUNID = sys.argv[13]
RUNNING_STATUS_CODE = sys.argv[14]
# TODO: CA injection

def end_run_with_code(code, output, error):
    cert_info = (CERT_PATH, KEY_PATH)
    event = SUCCESS_EVENT_TYPE if code == 0 else FAIL_EVENT_TYPE
    status = SUCCESS_STATUS_CODE if code == 0 else FAIL_STATUS_CODE
    final_output = output if code == 0 else f"Run failed with code {code}\n\nError log:\n{error}\n\nLog:{output}"
    requests.post(EMIT_URL, json={
        "type": event,
        "data": ""
    }, cert=cert_info)
    requests.post(STATUS_URL, json={
        "runId": RUNID,
        "status": {
            "status": status,
            "finished_at": int(time.time()) * 1000
        },
        "output": final_output,
        "errors": error
    }, cert=cert_info)
    exit_with_code(code)

# indicate run is running (there is no event for that)
requests.post(STATUS_URL, json={
    "runId": RUNID,
    "status": {
        "status": RUNNING_STATUS_CODE,
    },
}, cert=(CERT_PATH, KEY_PATH))

# -u to use unbuffered output
CMD = [sys.executable, '-u', FILE_TO_EXECUTE]

PROCESS = subprocess.Popen(
    CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
)

BUFFER = OutputBuffer(BUFFER_MAX, BUFFER_FLUSH_SECS, RequestFlushHandler(EMIT_URL, OUTPUT_EVENT_TYPE, (CERT_PATH, KEY_PATH)), True)

def end_run():
    process_status_code = PROCESS.wait()
    end_run_with_code(process_status_code, BUFFER.stdout, BUFFER.stderr)


SELECTOR = selectors.DefaultSelector()
SELECTOR.register(PROCESS.stdout, selectors.EVENT_READ)
SELECTOR.register(PROCESS.stderr, selectors.EVENT_READ)

PROCESS.stdin.write(bytes(input(), 'utf8'))
PROCESS.stdin.write(b'\n')
PROCESS.stdin.flush()

while True:
    for key, _ in SELECTOR.select():
        data = key.fileobj.read1(BUFFER_MAX).decode()
        if not data:
            BUFFER.flush_buffer()
            end_run()
        if key.fileobj is PROCESS.stdout:
            # copy output in case status request needs the data later
            print(data, file=sys.stdout, end='')
            BUFFER.register_stdout(data)
        else:
            print(data, file=sys.stderr, end='')
            BUFFER.register_stderr(data)