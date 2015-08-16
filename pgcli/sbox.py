import sys
import string
import re
import random
import time

def date_in(period, col_name = 'date', interval='month', ):
  start = period[:4] + '-' + period[-2:] + '-01'
  return "%s >= '%s' AND %s < (date '%s' + interval '1 %s')" % (col_name, start, col_name, start, interval)

