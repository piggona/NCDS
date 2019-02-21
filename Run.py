import os

from user_simul.userClass import *

if __name__ == "__main__":
    user = userClass(169020)
    response = user.user_read()
    print(response)