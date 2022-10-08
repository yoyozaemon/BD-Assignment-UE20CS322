import sys

for ln in sys.stdin:
    ln = ln.strip()
    if ln[0] != "#":
        try:
            frm_nod, to_nod = ln.split()
            frm_nod = frm_nod.strip()
            to_nod = to_nod.strip()
        except:
            continue
        print(frm_nod "\t" to_nod)
