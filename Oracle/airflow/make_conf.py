''' make_conf.py - Make JSON command-line config from .ini file
'''
from ConfigParser import SafeConfigParser
from json import dumps


def main(get_cfg):
    print r"%s" % (
        dumps(dict([(section, dict([(k, v)
                                    for k, v in values.items()
                                    if k != '__name__']))
                    for section, values in
                    dict(get_cfg()).items()])))


if __name__ == '__main__':
    def _tcb():
        from sys import argv

        def get_cfg():
            with open(argv[1], 'r') as fin:
                cfg = SafeConfigParser()
                cfg.readfp(fin)
                rcfg = cfg._sections
            return rcfg
        main(get_cfg=get_cfg)
    _tcb()
