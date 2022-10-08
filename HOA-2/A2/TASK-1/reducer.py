#!/usr/bin/python3

import sys

out_dir = sys.argv[0].strip()

pre_nod = None
pre_nod_output_link = list()

with open(out_dir, "w") as w_file:
    for ln in sys.stdin:
        ln = ln.strip()
        try:
            frm_nod, to_nod = ln.split()
            frm_nod = frm_nod.strip()
            to_nod = to_nod.strip()
        except:
            continue

        if pre_nod is None:
            pre_nod = frm_nod
            pre_nod_output_link.append(to_nod)

        elif pre_nod == frm_nod:
            pre_nod_output_link.append(to_nod)

        else:
            pre_nod_output_link.sort()
            print(f"{pre_nod}\t{pre_nod_output_link}")
            w_file.write(f"{pre_nod}, 1\n")
            pre_nod = frm_nod
            pre_nod_output_link.clear()
            pre_nod_output_link.append(to_nod)

    pre_nod_output_link.sort()
    print(f"{pre_nod}\t{pre_nod_output_link}")
    w_file.write(f"{pre_nod}, 1\n")