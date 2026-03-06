import time

from core import globalSettings, globalLogger
from classes import base
class output(base.base):

    def __init__(self,**kwargs):
        self.id = kwargs.get("id")
        self.logger = globalLogger.getLogger(__name__,kwargs.get("log_level",globalSettings.args.log_level))
        self.trace = kwargs.get("trace",False)
        super().__init__(**kwargs)

    def processHandler(self,event,stack=[]):
        eventStartTime = time.perf_counter_ns()
        if self.trace and type(event) == dict:
            if "__accept__" not in event:
                event["__accept__"] = { }
            event["__accept__"]["trace"] = stack
        retries = 0
        while retries < globalSettings.max_retries:
            try:
                self.process(event)  # Call your normal process method
                break  # Success, exit loop
            except SystemExit as e:
                if e.code == 2:
                    delay = globalSettings.base_delay * (2 ** retries)
                    self.logger.log(40, f"Plugin failed with sys.exit(2), retrying in {delay}s", {"retries": retries})
                    time.sleep(delay)
                    retries += 1
                else:
                    raise
            except Exception as e:
                self.logger.log(50, f"Unexpected error: {e}", {})
                break
        else:
            self.logger.log(50, "Max retries reached, giving up", {})
        self.updateProcessStats(eventStartTime)
        
    def process(self,event):
        pass
