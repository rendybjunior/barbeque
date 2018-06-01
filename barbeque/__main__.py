"""Main class"""
import sys
from barbeque.sample import inc

def main():
  """Main entry point"""
  print("hello world " + str(inc(5)))

if __name__ == '__main__':
  sys.exit(main())
