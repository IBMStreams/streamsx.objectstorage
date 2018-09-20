#!/usr/bin/env bash
echo clean-up standalone processes 
ps -ef | grep output/bin/standalone | grep /tmp/ | grep -v grep | awk '{print $2}' | xargs -r kill -9
