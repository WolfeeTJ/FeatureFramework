# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""
import json
import jsonpath

def process_online_vars(in_body_json):
    print("process_online_vars")
    var_a = jsonpath.jsonpath(in_body_json, '$.a')
    print("var_a: " + str(var_a))
