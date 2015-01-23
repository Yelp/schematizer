def yelp_bootstrap():
    """bootstrap the yelp python execution environment

    Add /nail/live/yelp via the site module.
    This can be overridden, on dev playgrounds, by setting
    YELPCODE to the directory where the code you want
    to use lives.

    See CEP25 for more details.
    """
    import sys
    import os
    import site

    if 'YELPCODE' in os.environ:
        # YELPCODE set: call addsitedir with the value of it
        basepath = os.environ['YELPCODE']
    else:
        # we find our basepath by looking for a
        # .yelpcode file, starting from $PWD and traversing upwards,
        # until the first VFS boundary.
        def get_filesystem_id(path):
            try:
                return os.stat(path).st_dev
            except Exception:
                return None

        directory = os.getcwd()
        orig_fs_id = fs_id = get_filesystem_id(directory)
        prev_directory = None
        while (
            fs_id is not None and
            orig_fs_id == fs_id and
            directory != prev_directory
        ):
            if os.path.exists(os.path.join(directory, '.yelpcode')):
                # Found it!
                basepath = directory
                break
            else:
                prev_directory = directory
                directory = os.path.dirname(directory)
                fs_id = get_filesystem_id(directory)
        else:
            # $YELPCODE unset and .yelpcode not found: use the global default
            if os.path.exists('/nail/live/yelp'):
                basepath = '/nail/live/yelp'
            else:
                basepath = None

    # $YELPCODE empty or .yelpcode not found: bail
    if not basepath:
        return

    # ensure the environment always reflects reality.
    os.environ['YELPCODE'] = basepath

    # find out the absolute path of the basepath, no path components
    # should be symbolic links.  this avoids race conditions where
    # the symlink's value changes
    realbasepath = os.path.realpath(basepath)

    # nothing to do if it doesn't exist
    if not os.path.exists(realbasepath):
        return

    # Add any .pth files in added sitedirs
    recursive_addpaths(sys.path, [realbasepath], site.addsitedir)

    # addsitedir only adds stuff to the end of sys.path
    # so move anything added above (even stuff added with .pth files),
    # that starts with realbasepath, to the front of sys.path but behind
    # the value of PYTHONPATH
    pp = []
    try:
        if 'PYTHONPATH' in os.environ and os.environ['PYTHONPATH']:
            lastppentry = os.environ['PYTHONPATH'].split(":")[-1]
            pp = sys.path[:sys.path.index(lastppentry)+1]
    except:
        pass

    mine = [x for x in sys.path if x.startswith(realbasepath)]
    theirs = [x for x in sys.path if not x.startswith(realbasepath) and x not in pp]
    sys.path = pp + mine + theirs


def recursive_addpaths(paths, new_paths, addpath):
    for path in new_paths:
        prev_paths = frozenset(paths + new_paths)
        #print 'ADDING PATH:', path
        addpath(path)
        discovered = [p for p in paths if p not in prev_paths]
        #print 'DISCOVERED:', discovered
        new_paths.extend(discovered)
