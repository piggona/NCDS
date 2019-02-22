import os
import time

from user_simul.userClass import *
from user_simul.user_generator import *
from user_simul.user_simulator import *

if __name__ == "__main__":
    # user_simulator(5)
    while True:
        user_simulator(5)
        time.sleep(10)
        print("休眠10s")
