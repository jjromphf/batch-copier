import os
from multiprocessing.pool import ThreadPool
import shutil
import csv
from functools import partial
from threading import Lock
from datetime import datetime

_csv_lock = Lock()
DEFAULT_MASTER_FILE_EXTENSIONS = set(
    [".log", ".mkv", ".xml", ".gz", ".framemd5", ".md5"]
)


def get_all_master_folders(folder, **kwargs):
    extensions = kwargs.get("extensions", None)
    extensions = extensions if extensions else DEFAULT_MASTER_FILE_EXTENSIONS
    master_folders = set()
    for dirname, dirnames, files in os.walk(folder):
        for f in files:
            ext = os.path.splitext(f)[1]
            if ext in extensions:
                master_folders.add(dirname)
                break
    return master_folders


def skip_subfolder(folder, files, **kwargs):
    hint = kwargs.get("hint", "")
    extensions = kwargs.get("extensions", None)
    extensions = extensions if extensions else DEFAULT_MASTER_FILE_EXTENSIONS
    if hint:
        return [
            f
            for f in files
            if os.path.isdir(os.path.join(folder, f))
            or os.path.splitext(f)[1] not in extensions
            or hint not in f
        ]
    return [
        f
        for f in files
        if os.path.isdir(os.path.join(folder, f))
        or os.path.splitext(f)[1] not in extensions
    ]


def copy_folder(to_copy, **kwargs):
    writer = kwargs.get("writer", None)
    ignore_func = kwargs.get("ignore_func", None)
    ignore_func = ignore_func if ignore_func else skip_subfolder
    try:
        print("Copying contents of {} to {}".format(to_copy["src"], to_copy["dst"]))
        new_folder = shutil.copytree(**to_copy, ignore=ignore_func)
        copied_files = os.listdir(new_folder)
        if len(copied_files) == 0:
            shutil.rmtree(new_folder)
        else:
            if writer:
                with _csv_lock:
                    writer.writerow(
                        {
                            **to_copy,
                            **{"files": (" | ").join(copied_files), "error": None},
                        }
                    )
    except (shutil.Error, FileExistsError) as errors:
        print("Unable to copy folder {}. Reason: {}".format(to_copy["src"], errors))
        if writer:
            with _csv_lock:
                writer.writerow({**to_copy, **{"error": errors}})


def get_files_to_be_moved(folder, extensions, hint):
    res = set()
    files = os.listdir(folder)
    for f in files:
        ext = os.path.splitext(f)[1]
        if hint:
            if ext in extensions and hint in f:
                res.add(f)
        else:
            if ext in extensions:
                res.add(f)
    return res


def copy_folders(folders, dest, **kwargs):
    dry_run = kwargs.get("dry_run", None)
    hint = kwargs.get("hint", None)
    extensions = kwargs.get("extensions", None)
    csv_name = "copy_report_{}.csv".format(datetime.now().strftime("%H%M_%d%m%y"))
    with open(csv_name, "w") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["src", "dst", "files", "error"])
        writer.writeheader()
        to_copy = [
            {"src": folder, "dst": os.path.join(dest, os.path.basename(folder))}
            for folder in folders
        ]
        if dry_run:
            print("Running report - nothing will be copied")
            for tc in to_copy:
                writer.writerow(
                    {
                        **tc,
                        **{"files": get_files_to_be_moved(tc["src"], extensions, hint)},
                    }
                )
        else:
            ignore_func = partial(skip_subfolder, hint=hint, extensions=extensions)
            copy_func = partial(copy_folder, writer=writer, ignore_func=ignore_func)
            with ThreadPool(4) as pool:
                pool.map(copy_func, to_copy)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Copy master file folders to a new directory"
    )
    parser.add_argument(
        "-s",
        "--source",
        help="Source directory to search for master files",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-d",
        "--destination",
        help="Destination directory to copy master folders to",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-e",
        "--extensions",
        help="Comma separated list of extensions to \
        figure out whether or not parent folder needs to be moved",
        required=False,
        type=str,
    )
    parser.add_argument(
        "-fh",
        "--file_hint",
        help="Hint contained in the file name to \
        figure out whether or not it needs to be moved",
        required=False,
        type=str,
    )
    parser.add_argument(
        "-dr",
        "--dry_run",
        help="Will not copy, just export the csv report",
        action="store_true",
        required=False,
    )
    parser.set_defaults(dry_run=False)
    args = vars(parser.parse_args())
    source = args["source"]
    destination = args["destination"]
    extensions = args["extensions"]
    hint = args["file_hint"]
    dry_run = args["dry_run"]
    ext = None
    if extensions:
        ext = extensions.split(",")
        if len(ext) == 0:
            ext = None
    folders = get_all_master_folders(source, extensions=ext)
    copy_folders(folders, destination, dry_run=dry_run, hint=hint, extensions=ext)
