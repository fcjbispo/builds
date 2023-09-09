from fbpyutils import file as F

_dirname = F.os.path.dirname(__file__)

CERTIFICATES = {
    f.split(F.os.path.sep)[-1].split('.')[0]: f
    for f in F.find(_dirname, '*.pem')
}

