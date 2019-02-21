import os

from user_simul.userClass import *

if __name__ == "__main__":
    user = userClass(1)
    response = user.get_recommend()
    print(response)