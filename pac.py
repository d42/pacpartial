import sys
import tarfile
import logging
import os
import re

from itertools import product, chain
from collections import defaultdict, deque
from hashlib import md5

import requests

logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


DEFAULT_SERVER = "http://arch.tamcore.eu/$repo/os/$arch"

ARCHS = ['i686', 'x86_64']
REPOS = ['core', 'extra', 'community']


def alpm_parse(file):
    entries = {}
    last_key = None
    for line in file.readlines():
        line = line.decode('utf-8').strip()
        if not line:
            continue

        if line[0] == '%' and line[-1] == '%':
            last_key = line[1:-1].lower()
        else:
            value = (line,)
            entries[last_key] = entries.get(last_key, tuple()) + value

    return entries


def delfile(path):
        try:
            os.remove(path)
        except:
            pass


def download(remote_path, local_path):
    print("%s -> %s" % (remote_path, local_path))
    if os.path.exists(local_path):
        return

    if not os.path.exists(local_path):
        content = requests.get(remote_path).content
        with open(local_path, 'wb') as file:
            file.write(content)
    else:
        with open(local_path, 'rb') as file:
            content = file.read()


def md5_file(md5sum, local_path):
    with open(local_path, 'rb') as file:
        content = file.read()
    m = md5()
    m.update(content)
    return m.hexdigest() == md5sum


class Package:
    __slots__ = ('filename', 'name', 'version',
                 'desc', 'csize', 'isize', 'md5sum',
                 'sha256sum', 'pgpsig', 'url', 'license',
                 'arch', 'builddate', 'packager', 'replaces', 'groups',
                 'base',
                 # DEPSFILE
                 'depends', 'makedepends', 'checkdepends', 'optdepends',
                 'conflicts', 'provides',

                 'remote_url', 'local_path')

    def __init__(self, name, descbuf=None, depsbuf=None):
        for s in self.__slots__:
            setattr(self, s, None)
        for v in ['groups', 'makedepends', 'checkdepends', 'depends',
                  'optdepends', 'conflicts', 'provides']:
            setattr(self, v, tuple())
        self.name = name
        if descbuf:
            self.parse_desc(descbuf)
        if depsbuf:
            self.parse_deps(depsbuf)

    def parse_desc(self, descbuf):
        for k, v in alpm_parse(descbuf).items():
            if k == 'groups':
                setattr(self, k, v)
            else:
                setattr(self, k, v[0])

    def parse_deps(self, depsbuf):

        for k, v in alpm_parse(depsbuf).items():
            v = tuple(re.match('[^><=:]+', e).group(0) for e in v)
            setattr(self, k, v)


class Repo:
    __slots__ = ['packages', 'mirrored_packages',
                 'groups', 'name', 'arch', 'local_path', 'server', 'provides']

    def __init__(self, name, arch, server, repo_root=''):
        self.name = name
        self.arch = arch
        self.local_path = os.path.join(repo_root, name, 'os', arch)
        self.server = server.replace('$repo', name).replace('$arch', arch)
        self.packages = dict()
        self.groups = defaultdict(list)
        self.provides = defaultdict(list)
        self.mirrored_packages = dict()

        try:
            os.makedirs(self.local_path)
        except:
            pass

        self.initialize_db()

    def get_db_file(self):
        db_path = os.path.join(self.local_path, self.name + '.db')

        if not os.path.exists(db_path):
            download(os.path.join(self.server, self.name + '.db'), db_path)
        return open(db_path, 'rb')

    def initialize_db(self):
        db_fobj = self.get_db_file()
        dbfile = tarfile.open(fileobj=db_fobj)
        for finfo in dbfile:
            name = '-'.join(finfo.name.split('-')[:-2])
            path = finfo.name

            if finfo.isdir():
                self.packages[name] = Package(name)
            else:
                file = dbfile.extractfile(finfo)
                package = self.packages[name]
                filename = os.path.basename(path)

                if filename == 'desc':
                    package.parse_desc(file)
                    url = os.path.join(self.server, package.filename)
                    local = os.path.join(self.local_path, package.filename)

                    package.remote_url = url
                    package.local_path = local
                elif filename == 'depends':
                    package.parse_deps(file)

        for name, pkg in self.packages.items():
            for g in pkg.groups:
                self.groups[g].append(name)

            for p in pkg.provides:
                self.provides[p].append(name)

    def download(self, pkgname):
        packages = self.resolve_name(pkgname)
        necessary_packages = self.resolve_deps(packages)

        for package_name in necessary_packages:
            print(package_name)

    def __contains__(self, pkgname):
        containers = [self.packages, self.provides, self.groups]
        return any(pkgname in e for e in containers)


class Mirror:
    __slots__ = ['repos', 'mirrored']

    def __init__(self, repo_names=REPOS, server=DEFAULT_SERVER, archs=ARCHS):
        self.repos = {}

        for name, arch in product(repo_names, archs):
            repo = Repo(name, arch, server)
            self.repos[(name, arch)] = repo

    def download(self, package_or_group, optdepends=False):
        packages = self.resolve_name(package_or_group)
        necessary_packages = self.resolve_deps(packages, optdepends=optdepends)
        for package in necessary_packages:
            download(package.remote_url, package.local_path)

    def resolve_name(self, pkgname, important=True):
        packages = set()
        found_packages = None
        for repo in self.repos.values():
            if pkgname in repo.packages:
                found_packages = [pkgname]
            elif pkgname in repo.provides:
                found_packages = repo.provides[pkgname]
            elif pkgname in repo.groups:
                found_packages = repo.groups[pkgname]
            if found_packages:
                packages.update(repo.packages[p] for p in found_packages)
                found_packages = None
        if packages or not important:
            return packages

        raise ValueError("%s not found in any repo" % pkgname)

    def resolve_deps(self, packages, optdepends):
        known_packages = set()
        packages = deque(packages)
        while packages:  # TODO: muh efficiency :333
            p = packages.popleft()

            if p in known_packages:
                continue
            known_packages.add(p)

            for d_name in chain(p.depends):
                for d in self.resolve_name(d_name):
                    packages.append(d)

            if optdepends:
                for d_name in chain(p.optdepends):
                    for d in self.resolve_name(d_name, important=False):
                        packages.append(d)

        return tuple(known_packages)


m = Mirror()

m.download('base', optdepends=False)
m.download('base-devel', optdepends=False)
m.download('archlinux-keyring')
m.download('openssh')
m.download('syslinux')
