# recursion
def sum(arr):
    if len(arr) == 0:
        return 0
    else:
        return arr[0] + sum(arr[1:])

def mmax(arr):
    if len(arr) == 2:
        return arr[0] if arr[0] > arr[1] else arr[1]
    sub_max = mmax(arr[1:])
    return arr[0] if arr[0] > sub_max else sub_max


def one_to_n(num):
    print(num)
    if num == 1:

        return
    else:
        print(num)
        return one_to_n(num - 1)

def main():
    s = 1
    def one_to_n(num):
        nonlocal s
        print(s)
        if num + s - num == 5:
            return
        else:
            s += 1
            return one_to_n(num)
    one_to_n(5)

def akkerman(m, n):
    if m == 0:
        return n + 1
    elif m > 0 and n == 0:
        return akkerman(m - 1, 1)
    elif m > 0 and n > 0:
        return akkerman(m - 1, akkerman(m, n - 1))


def stepen(num):
    if num == 1:
        print('yes')
        return
    elif num < 1:
        print('no')
        return
    return stepen(num / 2)


def nat_sum(num):
    if num < 10:
        return num
    return num % 10 + nat_sum(num // 10)


def quicksort(arr):
    import random
    if len(arr) < 2:
        return arr
    else:
        pivot = arr.pop(random.choice(range(0, len(arr))))
        less = [i for i in arr if i <= pivot]
        greater = [i for i in arr if i > pivot]

        return quicksort(less) + [pivot] + quicksort(greater)

quicksort([10,4,2,6,9,3,6,1])

a = [5,2,4,6,1,3]
def insert_sort(array):
    for i in range(1, len(array)):
        current = array[i]
        position = i
        while position > 0 and array[position - 1] > current:
            array[position] = array[position - 1]
            position -= 1
        array[position] = current

    return array
insert_sort(a)
