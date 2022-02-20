# Task 1. Hello world
print('Task 1. Hello world')
print("Hello, World!")

# Task 2. Data types
print(f'\nTask 2. Data types')
mystring = "hello, man"
myfloat = 3.14
myint = 1
# testing code
if "hello" in mystring:
    print("String: %s" % mystring)
if isinstance(myfloat, float) or myfloat == 10.0:
    print("Float: %f" % myfloat)
if isinstance(myint, int) or myint == 20:
    print("Integer: %d" % myint)

# Task 3. Lists
print(f'\nTask 3. Lists')
numbers = [x for x in range(1, 4)]
strings = ["hello", "world"]
names = ["John", "Eric", "Jessica"]
second_name = names[1]
# this code should write out the filled arrays and the second name in the names list (Eric).
print(numbers)
print(strings)
print("The second name on the names list is %s" % second_name)

# Task 4. Basic operators
print(f'\nTask 4. Basic operators')
x = object()
y = object()
x_list = [x] * 10
y_list = [y] * 10
big_list = x_list + y_list
print("x_list contains %d objects" % len(x_list))
print("y_list contains %d objects" % len(y_list))
print("big_list contains %d objects" % len(big_list))
# testing code
if x_list.count(x) == 10 and y_list.count(y) == 10:
    print("Almost there...")
if big_list.count(x) == 10 and big_list.count(y) == 10:
    print("Great!")

# Task 5. String_Formatting
print(f'\nTask 5. String_Formatting')
data = ("John", "Doe", 53.44)
format_string = "Hello %s %s. Your current balance is $%s."
print(format_string % data[:])

# Task 6. Basic_String_Operations
print(f'\nTask 6. Basic_String_Operations')
s = "Str 5678aa12345 ome!"
# Length should be 20
print("Length of s = %d" % len(s))
# First occurrence of "a" should be at index 8
print("The first occurrence of the letter a = %d" % s.index("a"))
# Number of a's should be 2
print("a occurs %d times" % s.count("a"))
# Slicing the string into bits
print("The first five characters are '%s'" % s[:5])  # Start to 5
print("The next five characters are '%s'" % s[4:9])  # 5 to 10
print("The thirteenth character is '%s'" % s[11])  # Just number 12
print("The characters with odd index are '%s'" % s[0::2])  # (0-based indexing)
print("The last five characters are '%s'" % s[-5:])  # 5th-from-last to end
# Convert everything to uppercase
print("String in uppercase: %s" % s.upper())
# Convert everything to lowercase
print("String in lowercase: %s" % s.lower())
# Check how a string starts
if s.startswith("Str"):
    print("String starts with 'Str'. Good!")
# Check how a string ends
if s.endswith("ome!"):
    print("String ends with 'ome!'. Good!")
# Split the string into three separate strings,
# each containing only a word
print("Split the words of the string: %s" % s.split(" "))

# Task 7. Conditions
print(f'\nTask 7. Conditions')
number = 16
second_number = 0
first_array = [1, 2, 3]
second_array = [1, 2]
if number > 15:
    print("1")
if first_array:
    print("2")
if len(second_array) == 2:
    print("3")
if len(first_array) + len(second_array) == 5:
    print("4")
if first_array and first_array[0] == 1:
    print("5")
if not second_number:
    print("6")

# Task 8. Loops
print(f'\nTask 8. Loops')
numbers = [
    951, 402, 984, 651, 360, 69, 408, 319, 601, 485, 980, 507, 725, 547, 544,
    615, 83, 165, 141, 501, 263, 617, 865, 575, 219, 390, 984, 592, 236, 105, 942, 941,
    386, 462, 47, 418, 907, 344, 236, 375, 823, 566, 597, 978, 328, 615, 953, 345,
    399, 162, 758, 219, 918, 237, 412, 566, 826, 248, 866, 950, 626, 949, 687, 217,
    815, 67, 104, 58, 512, 24, 892, 894, 767, 553, 81, 379, 843, 831, 445, 742, 717,
    958, 609, 842, 451, 688, 753, 854, 685, 93, 857, 440, 380, 126, 721, 328, 753, 470,
    743, 527
]
for number in numbers:
    if number % 2 == 0 and number <= 237:
        print(number)

# Task 9. Functions
print(f'\nTask 9. Functions')


# Modify this function to return a list of strings as defined above
def list_benefits():
    benefits = ["More organized code",
                "More readable code",
                "Easier code reuse",
                "Allowing programmers to share and connect code together"
                ]
    return benefits


# Modify this function to concatenate to each benefit - " is a benefit of functions!"
def build_sentence(benefit):
    return "%s is a benefit of functions!" % benefit


def name_the_benefits_of_functions():
    list_of_benefits = list_benefits()
    for benefit in list_of_benefits:
        print(build_sentence(benefit))


name_the_benefits_of_functions()

# Task 11. Functions
print(f'\nTask 11. Functions')
phonebook = {
    "John": 938477566,
    "Jack": 938377264,
    "Jill": 947662781
}
# your code goes here
phonebook['Jake'] = 938273443
del phonebook['Jill']
# testing code
if "Jake" in phonebook:
    print("Jake is listed in the phonebook.")
if "Jill" not in phonebook:
    print("Jill is not listed in the phonebook.")

# Task 12. Set
print(f'\nTask 12. Set')
a = ["Jake", "John", "Eric"]
b = ["John", "Jill"]
A = set(a)
B = set(b)
print(A.difference(B))

# Task 13. Import
print(f'\nTask 13. Import')
import colorama

colorama.init(autoreset=True)

from colorama import Back, Fore

print(f"{Fore.RED}Hello Color!")
print(f"{Back.RED}Hello Color!")



# Task 14. Modules_and_Packages
print(f'\nTask 14. Modules_and_Packages')
import re

all_functions = dir(re)
find_members = []
for fun in all_functions:
    if "find" in fun:
        find_members.append(fun)
print(find_members)

# Task 15. Time
print(f'\nTask 15. Time')
from time import time, ctime

t = time()
print(ctime(t))


# Task 16. Files
print(f'\nTask 16. Files')
file = open('text.txt', 'w')
squares = [value ** 2 for value in range(1, 11)]
for number in squares:
    file.write(str(number) + '\n')
file.close()

file = open('text.txt', 'r')
line = [line.strip() for line in file]
print(f'Your numbers are: {line}')
file.close()
