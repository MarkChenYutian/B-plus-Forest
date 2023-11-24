import random


def writeCase(output, length, valmax, ops, threadCnt):
    OUTPUT = output
    LENGTH = length
    VALMAX = valmax
    OPS    = ops
    TREE   = set()

    lines = []

    for _ in range(LENGTH):
        op     = random.choice(OPS)
        value  = random.randint(0, VALMAX)
        thread = random.randint(0, threadCnt - 1)
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
        lines.append(f"{op},{value},{expect},{thread}")

    with open(OUTPUT, "w") as f: f.write("\n".join(lines))

for i in range(10):
    writeCase(
        f"./large_{i}.case",
        1000000,
        2000,
        ["G", "I", "I", "I", "I", "D"],
        4
    )
for i in range(10):
    writeCase(
        f"./small_{i}.case",
        1000,
        10,
        ["G", "I", "I", "I", "I", "D"],
        1
    )
