# date:2019/2/22
# -*- coding: utf-8 -*-
# auth：haohao

import os
import json
import multiprocessing

from user_simul.userClass import *


def user_operate(user_id):
    user = userClass(user_id)
    user.user_read()


def user_simulator(amount):
    pass
