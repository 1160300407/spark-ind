import numpy as np
np.random.seed(42)

pool_start = 456789
lock_range = (100000, 200000)
garbage_range = (300000, 400000)

lhs_init_len = 1234567
tableLHSlen = 2345678
tableRHSlen = 3456789

lhs_path = "lhs.csv"
rhs_path = "rhs.csv"


class Pool():
    def __init__(self):
        self.i = pool_start - 1

    def __next__(self):
        self.i += 1
        return str(self.i)


class Attribute():
    def __init__(self, name):
        self.name = name
        self.values = []


class IND():
    def __init__(self, name, lhs, rhs):
        self.name = name
        self.lhs = lhs
        self.rhs = rhs
        self.locks = {}


a1 = Attribute('a1')
b1 = Attribute('b1')
c1 = Attribute('c1')
d1 = Attribute('d1')
e1 = Attribute('e1')

i1 = Attribute('i1')
j1 = Attribute('j1')
k1 = Attribute('k1')
l1 = Attribute('l1')
m1 = Attribute('m1')

a2 = Attribute('a2')
b2 = Attribute('b2')
c2 = Attribute('c2')
d2 = Attribute('d2')
e2 = Attribute('e2')
f2 = Attribute('f2')

i2 = Attribute('i2')
j2 = Attribute('j2')
k2 = Attribute('k2')
l2 = Attribute('l2')
m2 = Attribute('m2')
n2 = Attribute('n2')

a3 = Attribute('a3')
b3 = Attribute('b3')
c3 = Attribute('c3')
d3 = Attribute('d3')
e3 = Attribute('e3')
f3 = Attribute('f3')
g3 = Attribute('g3')

i3 = Attribute('i3')
j3 = Attribute('j3')
k3 = Attribute('k3')
l3 = Attribute('l3')
m3 = Attribute('m3')
n3 = Attribute('n3')
o3 = Attribute('o3')

A = IND('A', [a1, b1, c1, d1, e1], [i1, j1, k1, l1, m1])
B = IND('B', [a2, b2, c2, d2, e2, f2], [i2, j2, k2, l2, m2, n2])
C = IND('C', [a3, b3, c3, d3, e3, f3, g3], [i3, j3, k3, l3, m3, n3, o3])

inds = [A, B, C]
attributsLHS = [a1, b1, c1, d1, e1, a2, b2, c2, d2, e2, f2, a3, b3, c3, d3, e3, f3, g3]
attributsRHS = [i1, j1, k1, l1, m1, i2, j2, k2, l2, m2, n2, i3, j3, k3, l3, m3, n3, o3]
out_attributsLHS = np.random.permutation(attributsLHS)
out_attributsRHS = np.random.permutation(attributsRHS)


# init lhs generation
pool = Pool()
for i in range(lhs_init_len):
    for ind in inds:
        for attl in ind.lhs:
            y = next(pool)
            attl.values += [y]


for i, ind in enumerate(inds):
    keeptrackdict = {}
    for j in range(i+1, len(inds)):
        if inds[j].name not in ind.locks.keys():

            lockline = np.random.randint(len(ind.lhs[0].values))

            ind.locks.update({inds[j].name: lockline})
            inds[j].locks.update({ind.name: lockline})

            keeptrackdict[lockline] = keeptrackdict.get(lockline, []) + [j]

    for key, values in keeptrackdict.items():
        if len(values) >= 2:
            for i, val in enumerate(values):
                for j in range(i+1, len(values)):
                    inds[val].locks.update({inds[values[j]].name: key})
                    inds[values[j]].locks.update({inds[val].name: key})

# for ind in inds:
#     print(ind.name, ':', ind.locks)


for i, ind in enumerate(inds):
    values = list(zip(*[att.values for att in ind.lhs]))
    for vals in values:
        for attr, val in zip(ind.rhs, vals):
            attr.values += [val]
    for j in range(len(inds)):
        if i == j:
            continue
        for k in range(len(values)):
            if ind.locks.get(inds[j].name, -999) == k:
                for attr in inds[j].rhs:
                    attr.values += [0]
            else:
                for attr in inds[j].rhs:
                    attr.values += [-1]

mustLHSlen = len(attributsLHS[0].values)
mustRHSlen = len(attributsRHS[0].values)
mustLHS = list(np.random.permutation(mustLHSlen))
mustRHS = list(np.random.permutation(mustRHSlen))


def draw_index_lhs():
    if np.random.rand() <= (mustLHSlen / tableLHSlen) and mustLHS:
        idx = mustLHS.pop()
    else:
        idx = np.random.randint(mustLHSlen)
    return idx


def draw_index_rhs():
    if np.random.rand() <= (mustRHSlen / tableRHSlen) and mustRHS:
        idx = mustRHS.pop()
    else:
        if np.random.rand() <= 0.5:
            idx = np.random.randint(mustRHSlen)
        else:
            idx = -1
    return idx


def construct_line_lhs(idx):
    line = []
    for attr in out_attributsLHS:
        line.append(attr.values[idx])
    return " ".join(line)


def construct_line_rhs(idx):
    line = []
    for attr in out_attributsRHS:
        val = -1 if idx == -1 else attr.values[idx]
        if val == -1:
            val = str(np.random.randint(*garbage_range))
        elif val == 0:
            val = str(np.random.randint(*lock_range))
        line.append(val)
    return " ".join(line)


def write_lhs():
    with open(lhs_path, 'w') as f:
        for i in range(tableLHSlen):
            index = draw_index_lhs()
            line = construct_line_lhs(index)
            f.write(line + "\n")

        if mustLHS:
            for e in mustLHS:
                index = e
                line = construct_line_lhs(index)
                f.write(line + "\n")


def write_rhs():
    with open(rhs_path, 'w') as f:
        for i in range(tableRHSlen):
            index = draw_index_rhs()
            line = construct_line_rhs(index)
            f.write(line + "\n")

        if mustRHS:
            for e in mustRHS:
                index = e
                line = construct_line_rhs(index)
                f.write(line + "\n")


write_lhs()
write_rhs()