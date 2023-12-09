from testGeneratev2 import Trace


def loadCase(name: str):
    with open(name, "r") as f: lines = f.read().strip().split("\n")
    case = [Trace.deserialize(line) for line in lines]
    return case


# def opDistribution(case):
#


