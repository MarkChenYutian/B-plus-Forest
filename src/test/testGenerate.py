import random

for i in range(100):
    str_ = "./big" + str(i) + ".case"
    # OUTPUT = "./3.case"
    OUTPUT = str_
    LENGTH = 200000
    VALMAX = 100
    OPS    = ["G", "I", "I", "I", "I", "D"]
    TREE   = set()

    lines = []

    for _ in range(LENGTH):
        op     = random.choice(OPS)
        value  = random.randint(0, VALMAX)
        while (op == "I" and value in TREE):
            op     = random.choice(OPS)
            value  = random.randint(0, VALMAX)
        
        expect = "NONE"
        if op == "G":
            if value in TREE: expect = str(value)
        elif op == "I":
            TREE.add(value)
        elif op == "D":
            if value in TREE: expect = str(value)
            TREE.discard(value)
        lines.append(f"{op},{value},{expect}")

    with open(OUTPUT, "w") as f: f.write("\n".join(lines))
