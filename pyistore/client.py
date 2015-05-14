import requests
import json
import urllib
import contextlib


class Server(object):

    chunksize = 4096

    def __init__(self, addr):
        """
        addr: https?://<host>:<port>
        """
        self.addr = addr

    def makeurl(self, path, appl=None):
        url = self.addr + urllib.quote(path)
        if appl is not None:
            url = appl.append(url)
        return url

    def selfurl(self, path, appl=None):
        url = 'self://' + path.replace('%', '%25').replace('?', '%3F')
        if appl is not None:
            url = appl.append(url)
        return url

    def read(self, path, appl=None):
        url = self.makeurl(path, appl)
        with contextlib.closing(requests.get(url, stream=True)) as r:
            for chunk in r.iter_content(chunk_size=self.chunksize):
                yield chunk

    def post(self, path, metadata=None):
        return self._putpost(path, True, metadata)

    def put(self, path, metadata=None):
        return self._putpost(path, False, metadata)

    def delete(self, path):
        url = self.makeurl(path)
        resp = requests.delete(url)
        return resp

    def _putpost(self, path, ispost, metadata):
        url = self.makeurl(path)
        payload = {}
        if metadata is not None:
            payload['metadata'] = json.dumps(metadata)

        if ispost:
            resp = requests.post(url, data=payload)
        else:
            resp = requests.put(url, data=payload)

        return resp.json()

    def list(self, path):
        url = self.makeurl(path)
        resp = requests.get(url)
        return resp.json()


class Apply(object):

    def __init__(self, name, **kwargs):
        self.name = name
        self.params = kwargs

    def query(self):
        params = self.params.copy()
        params['apply'] = self.name
        return self._concatparam(params, '=', '&')

    def append(self, path):
        return path + '?' + self.query()

    @classmethod
    def subparams(cls, subparams):
        return cls._concatparam(subparams, '/', ',')

    @classmethod
    def _concatparam(cls, params, sep1, sep2):
        '''
        {key1}={val1}&{key2}={val2}
        where '=' is sep1 and '&' is sep2
        '''
        buf = []
        for key, values in params.items():
            if not isinstance(values, list):
                values = [values]
            for val in values:
                buf.append('{}{}{}'.format(key, sep1, urllib.quote(str(val))))
        return sep2.join(buf)

if __name__ == '__main__':
    istore = Server('http://localhost:8592')
    istore.delete('/pyistore/')
    path = '/pyistore/img/' \
        'http://upload.wikimedia.org/wikipedia/en/a/a9/Example.jpg'
    istore.post(path, metadata={'name': 'Expedia'})
    with open('/tmp/test.jpg', 'wb') as f:
        for chunk in istore.read(path):
            f.write(chunk)

    print istore.list('/pyistore/img/')
    invert = Apply('invert')
    path = '/pyistore/img/' + istore.selfurl(path, invert)
    istore.post(path)
    with open('/tmp/test2.jpg', 'wb') as f:
        for chunk in istore.read(path):
            f.write(chunk)

    rect1 = Apply.subparams({
        'x1': 50, 'y1': 50, 'x2': 100, 'y2': 100,
        'r': 255, 'g': 255, 'b': 99,
    })
    rect2 = Apply.subparams({
        'x1': 110, 'y1': 110, 'x2': 140, 'y2': 140,
        'r': 99, 'g': 255, 'b': 255,
    })
    drect = Apply('drawRect', rects=[rect1, rect2])
    with open('/tmp/test3.jpg', 'wb') as f:
        for chunk in istore.read(path, drect):
            f.write(chunk)

    print istore.list('/pyistore/img/')
