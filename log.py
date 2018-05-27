import datetime


def log_to_scr(mmodule, string):
    print(str(datetime.datetime.now()) + " - " + mmodule + " - " + string)
