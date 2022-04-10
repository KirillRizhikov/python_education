"""HANGMAN game"""


import random
import nltk
import requests

nltk.download('averaged_perceptron_tagger')

WORD_SITE = 'http://www.mit.edu/~ecprice/wordlist.10000'


def get_word():
    """function that finds the word for game"""
    response = requests.get(WORD_SITE)
    words = response.content.splitlines()

    noun = 0
    while noun != 'NN':
        rand_word = str(random.choice(words)).replace("b'", "", 1)
        rand_word = rand_word.removesuffix("'")
        tokens = nltk.word_tokenize(rand_word)
        tagged = nltk.pos_tag(tokens)
        noun = tagged[0][1]
    return rand_word


hangman = [
    "    _____\n    |    |\n         |\n         |\n         |\n---------------",
    "    _____\n    |    |\n    O    |\n         |\n         |\n---------------",
    "    _____\n    |    |\n    O    |\n    |    |\n         |\n---------------",
    "    _____\n    |    |\n    O    |\n   /|    |\n         |\n---------------",
    "    _____\n    |    |\n    O    |\n   /|\   |\n         |\n---------------",
    "    _____\n    |    |\n    O    |\n   /|\   |\n   /     |\n---------------",
    "    _____\n    |    |\n    O    |\n   /|\   |\n   / \   |\n---------------",
]


def game():
    """Main game function"""
    word = get_word().upper()
    write_words(word)
    tries = len(hangman)
    display = '_' * len(word)

    print(f'Guess the {len(word)}-letter word or go to the HANGMAN')

    game_over = False
    guess_letters = []
    while not game_over:
        print('You have ' + str(tries) + ' remaining.')
        print(display)
        guess = input('Give me a letter: ').upper()
        guess_letters.append(guess.upper())

        i = 0
        if guess in word:
            while word.find(guess, i) != -1:
                i = word.find(guess, i)
                display = display[:i] + guess.capitalize() + display[i + 1:]
                i += 1
            print('Correct')

        else:
            print('Sorry, wrong guess')
            print(hangman[-tries])
            tries -= 1
        print('your previous guesses: ', guess_letters)

        if word == display:
            print("You win! The word was " + word)
            game_over = True

        if tries == 0:
            print(f'Sorry, you are DEAD \n The word is {word.upper()}')
            game_over = True


def write_words(word):
    """Function that writes used words to the text file"""
    with open("hangman_words.txt", "a+", encoding='utf-8') as file:
        file.write(f'\n{word}')
        file.close()


def read_words():
    """Function that shows used words from previous games """
    try:
        with open("hangman_words.txt", "r", encoding='utf-8') as file:
            contents = file.read()
            print('\n Used words:')
            print(contents)
    except FileNotFoundError:
        print('There is no stored words')


EXIT_GAME = False
while not EXIT_GAME:
    game_choice = input('\nHi Stranger. Choose: \n1 to PLAY \n2 to see Previous words \n3 to Exit')
    if game_choice == '1':
        game()
    elif game_choice == '2':
        read_words()
    elif game_choice == '3':
        print('Good bye!')
        EXIT_GAME = True
    else:
        print('Please, inter number from 1 to 3')
