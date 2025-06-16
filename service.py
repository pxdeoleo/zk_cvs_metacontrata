import win32serviceutil
import win32service
import win32event
import servicemanager
import threading
from sync_scheduler import start_scheduler


class SyncService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MetaCVSyncService"
    _svc_display_name_ = "MetaContrata CVSecurity Sync Service"

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.thread = threading.Thread(target=start_scheduler)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.thread.start()
        win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SyncService)
