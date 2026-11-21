"""Build add-only standard-library recovery from a pinned official CPython tag."""
import hashlib,json,zipfile,sys
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parents[1]
SOURCE_SHA256='37405eed50fd3038cef10c0aa595fcf662661e5a36e38678e4fc085dff7f9da1'
SOURCE_URL='https://github.com/python/cpython/archive/refs/tags/v3.11.0.zip'

def library_files(source):
    data=Path(source).read_bytes()
    if hashlib.sha256(data).hexdigest()!=SOURCE_SHA256:raise ValueError('Official source archive checksum differs; refusing to package')
    result={}
    with zipfile.ZipFile(source) as z:
        for name in z.namelist():
            if not name.startswith('cpython-3.11.0/Lib/') or name.endswith('/'):continue
            path=PurePosixPath(name)
            if '..' in path.parts or '\\' in name:raise ValueError('Unsafe source archive')
            # No tests, GUI applications, activation scripts or third-party packages.
            relative=PurePosixPath(*path.parts[1:])
            if any(p in {'test','tests','idlelib','tkinter','site-packages','venv'} for p in relative.parts[1:]):continue
            if relative.suffix not in {'.py','.whl'}:continue
            result[str(relative)]=z.read(name)
    for required in ['Lib/os.py','Lib/encodings/__init__.py','Lib/site.py','Lib/ensurepip/__init__.py']:
        if required not in result:raise ValueError('Incomplete source library')
    return result

def build(source,output):
    files=library_files(source)
    manifest=json.dumps({n:hashlib.sha256(v).hexdigest() for n,v in sorted(files.items())},sort_keys=True,indent=2).encode()
    with zipfile.ZipFile(source) as archive:files['PYTHON_LICENSE.txt']=archive.read('cpython-3.11.0/LICENSE')
    files['LIBRARY_SHA256.json']=manifest
    files['RESTORE_MISSING_LIBRARY.ps1']=(ROOT/'python-library-recovery/RESTORE_MISSING_LIBRARY.ps1').read_bytes()
    files['SOURCE.txt']=(f'Official CPython v3.11.0 standard library.\nSource: {SOURCE_URL}\nSource archive SHA256: {SOURCE_SHA256}\nOnly missing files are restored. No existing files are overwritten.\nNo running programs, scheduled tasks, account rules or risk journals are changed.\n').encode()
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
        for name,data in sorted(files.items()):
            info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,data)
    print('Recovery archive:',len(files),'files;',Path(output).stat().st_size,'bytes; SHA256',hashlib.sha256(Path(output).read_bytes()).hexdigest())

if __name__=='__main__':build(sys.argv[1],sys.argv[2])
