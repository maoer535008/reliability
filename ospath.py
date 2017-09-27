import os,sys

def lpath(path = ''):
    if path =='':
        return sys.path[0]
    else:
        return os.path.join(sys.path[0],path)

def upath(path = ''):
    if path =='':
        return os.path.split(sys.path[0])[0]
    else:
        return os.path.join(os.path.split(sys.path[0])[0],path)

def cpath(filename):
    pdir,name = os.path.split(filename)
    if not os.path.exists(pdir):
        os.makedirs(pdir)
    return filename
