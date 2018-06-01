"""Unit test for sample file"""
import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest
from barbeque.sample import inc

def test_main():
    assert inc(3) == 5
