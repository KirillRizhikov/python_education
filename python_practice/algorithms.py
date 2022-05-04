"""
Реализовать на Python:
- Binary search
- Quick sort (Iterative)
- Recursive factorial implementation
Покрытие реализаций тестами - обязательно.
"""


def binary_search(arr, number):
    """Binary search"""
    l_index = 0
    r_index = len(arr) - 1
    mid_index = 0

    while l_index <= r_index:
        mid_index = (l_index + r_index) // 2
        mid_number = arr[mid_index]

        if mid_number == number:
            return mid_index

        if mid_number < number:
            l_index = mid_index + 1
        else:
            r_index = mid_index - 1
    return -1


def quick_sort(arr):
    """Quick sort (Iterative)"""
    size = len(arr)
    for i in range(size - 1):
        min_index = i
        for j in range(min_index + 1, size):
            if arr[j] < arr[min_index]:
                min_index = j
        if i != min_index:
            arr[i], arr[min_index] = arr[min_index], arr[i]


def factorial(number):
    """Recursive factorial implementation"""
    if number < 0:
        raise ValueError
    if number in (0, 1):
        return 1
    else:
        return number * factorial(number - 1)
