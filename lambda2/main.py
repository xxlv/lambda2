import argparse
import sys
import logging
import os
try:
    import colorlog
except ImportError:
    pass

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    format      = '%(asctime)s - %(levelname)-8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    if 'colorlog' in sys.modules and os.isatty(2):
        cformat = '%(log_color)s' + format
        f = colorlog.ColoredFormatter(cformat, date_format,
              log_colors = { 'DEBUG'   : 'reset',       'INFO' : 'reset',
                             'WARNING' : 'bold_yellow', 'ERROR': 'bold_red',
                             'CRITICAL': 'bold_red' })
    else:
        f = logging.Formatter(format, date_format)
    ch = logging.StreamHandler()
    ch.setFormatter(f)
    root.addHandler(ch)

setup_logging()
log = logging.getLogger(__name__)



from lambda2 import server

def check_version():
    if sys.version_info < (3, 0):
        print("Python 3.0 or greater required (3.5 recommended). Please consider upgrading or "
              "using a virtual environment.")
        sys.exit(1)


def parse_args():
    """ Parse command line arguments and returns a configuration object.
    :return the configuration object, arguments accessed via dotted notation
    :rtype Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--node",action='store_true')
    parser.add_argument("--port", type=int, nargs='?', default=None)

    return parser.parse_args()


def main():
    config = parse_args()
    if config.node:
        server.Server().run(config.port)