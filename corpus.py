#!/usr/bin/env python3

import os
import time
from subprocess import call


def main():
    total_time = 0
    for file_name in os.listdir("./files/raw.en"):
        print(file_name)
        time_start = time.time()
        call(["go", "run", "client/client.go", "-put", file_name])
        time_end = time.time()

        print("push {} used {}".format(file_name, time_end - time_start))
        total_time += time_end - time_start
        time.sleep(0.5)

    print("total time: {}".format(total_time))


if __name__ == '__main__':
    main()
