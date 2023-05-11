#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from datetime import datetime


def say(message):
    print(f"{datetime.now()}: {message}", flush=True)
    sys.stdout.flush()
