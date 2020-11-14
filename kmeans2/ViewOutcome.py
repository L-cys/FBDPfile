#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 16:20:26 2020

@author: chenyuanshan
"""

import numpy  as np
import pandas as pd
import matplotlib as plt
df = pd.read_table("/Users/chenyuanshan/temp/kmeans/part-r-00000",header = None)
df = pd.concat([df, df[1].str.split(',', expand=True)], axis=1)
df.columns = ['center','original','x_axis','y_axis']
df['y_axis'] = pd.to_numeric(df.y_axis)
df['x_axis'] = pd.to_numeric(df.x_axis)
df.plot(kind = "scatter", x = "x_axis", y = "y_axis", c = "center", alpha = 0.4)