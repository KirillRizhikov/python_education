"""Написать на Python консольную версию Крестиков-Ноликов
Фичи:
1. Меню(играть, просмотреть лог побед, очистить лог побед, выход)
2. Ведение логов побед в файле. Пример: 12.10.2020 12:34 -
Победил Вася... (логи писать как в стандартный поток, так и в файл)
3. В начале каждой игры необходимо запросить имена игроков и
записывать в логи победы под этими именами
4. Добавить возможность сыграть еще одну партию после победы/поражения
с теми же игроками и тогда начать записывать счет по партиям
4. Код разбит на классы и методы (максимально используем ООП). Отдельно
реализована функция main.
5. Код соответствует ранее изученому код стайлу - pylint
6. Залить код на гит и сбросить ссылку в классрум"""
import abc
import logging
from abc import ABC, abstractclassmethod
import numpy as np

logger = logging.getLogger("Tic_tac")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('tic_tac_log.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


class Model():
    """Board class"""

    def __init__(self, size):
        self.size = size
        self.board = np.zeros((self.size, self.size, 2), dtype=int)
        self.mirror_board = np.zeros((self.size, self.size, 2), dtype=int)
        self.win = False

    def check_move(self, row, column):
        """Checks if move possible"""
        return self.board.sum(axis=2)[row, column] == 0

    def write_move(self, player, row, column):
        """Writes the move"""
        self.board[row, column, player] = 1

    def check_win(self, player):
        """Checks if Player win"""
        self.win = False
        for i in range(self.size):
            self.mirror_board[:, i, :] = self.board[:, -(i + 1), :]
        for row in range(self.size):
            if self.board[row, :, player].sum() == self.size:
                self.win = " you win horizontally!"
            if self.board[:, row, player].sum() == self.size:
                self.win = " you win vertically!"
        if np.trace(self.board[:, :, player]) == self.size or \
                np.trace(self.mirror_board[:, :, player]) == self.size:
            self.win = " you win diagonally!"
        return self.win

    def clear_board(self):
        """Clears the board"""
        self.board = np.zeros((self.size, self.size, 2), dtype=int)
        self.mirror_board = np.zeros((self.size, self.size, 2), dtype=int)


class Player:
    """Class Player"""

    def __init__(self, name: "Player", view):
        self.name = name
        self.choice = []
        self.choices = []
        self.view = view
        logging.debug('Player {} added'.format(self.name))

    def make_choice(self, board):
        """Makes move"""
        while True:
            self.choice = self.view.move_request(self.name, board)
            if board.check_move(*self.choice):
                break
            print("This sell is occupied")
        self.choices.append(self.choice)


class View(ABC):
    "View class"

    def __init__(self):
        self.player1 = "Tic"
        self.player2 = "Tac"

    def check_user_input(self, input):
        """Checks if int"""
        try:
            val = int(input)
        except ValueError:
            try:
                # Convert it into float
                val = False
                print("Input is a float  number. Number = ", val)
            except ValueError:
                print("No.. input is not a number. It's a string")
                val = False
        return val

    @abc.abstractmethod
    def show_log(self):
        """Shows log file"""
        pass

    @abc.abstractmethod
    def show_menu(self):
        """Shows menu"""
        pass

    @abc.abstractmethod
    def name_request(self):
        """Asks a name"""
        pass

    @abc.abstractmethod
    def show_bord(self, board):
        """Shows the board"""
        pass

    @abc.abstractmethod
    def ask_size(self):
        """Asks bord size"""
        pass

    @abc.abstractmethod
    def move_request(self, player, board):
        """Asks next move"""
        pass


class TerminalView(View):
    """TerminalView class"""

    def show_log(self):
        """Shows log file"""
        try:
            with open("tic_tac_log.log", "r", encoding='utf-8') as file:
                contents = file.read()
                print(contents)
        except FileNotFoundError:
            print('No log file')

    def show_menu(self):
        """Shows menu"""
        while True:
            try:
                menu_choice = int(input(
                    "Menu: \n1 - Play \n2 - Check win log \n3 - Clean win log \n4 - Exit \n"))
                if menu_choice not in range(1, 5):
                    raise TicTacValueError()
                break
            except TicTacValueError:
                print("Choice correct number")
        return int(menu_choice)

    def name_request(self):
        """Asks a name"""
        self.player1 = input("What's your name, Player 1?: ")
        self.player2 = input("What's your name, Player 2?: ")
        return self.player1, self.player2

    def show_bord(self, board):
        """Shows the board"""
        view_bord = (board[:, :, 0] * 1 + board[:, :, 1] * 2)
        temp_arr = np.array(
            ["%.2f" % w for w in view_bord.reshape(view_bord.size)])
        temp_arr = temp_arr.reshape(view_bord.shape)
        temp_arr = np.core.defchararray.replace(temp_arr, '0.00', ' ')
        temp_arr = np.core.defchararray.replace(temp_arr, '1.00', 'X')
        temp_arr = np.core.defchararray.replace(temp_arr, '2.00', 'O')
        return temp_arr

    def ask_size(self):
        """Asks bord size"""
        while True:
            size = input("Choose size of the board 1-**")
            size = self.check_user_input(size)
            if size and size != 0:
                break
            print("Enter a number 1-**")
        return size

    def move_request(self, player, board):
        """Asks next move"""
        while True:
            try:
                row_choice = input(
                    f"{player} make your move. \nRow (1-{board.size}): ")
                if row_choice not in [str(i) for i in range(1, board.size + 1)]:
                    raise TicTacValueError()
                break
            except TicTacValueError:
                print("Choice correct number")
        while True:
            try:
                column_choice = input(f"Column (1-{board.size}): ")
                if column_choice not in [str(i) for i in range(1, board.size + 1)]:
                    raise TicTacValueError()
                break
            except TicTacValueError:
                print("Choice correct number")
        return int(row_choice) - 1, int(column_choice) - 1


class TicTacValueError(Exception):
    """Choice correct number"""
    pass


class Controller():
    """Controller class"""

    def __init__(self, view):
        self.view = view
        self.choice = 1

    def clear_log(self):
        """Clears log file"""
        print("Cleaning log file")
        open('tic_tac_log.log', 'w').close()

    def menu(self):
        """Main menu options"""
        while not self.choice == 4:
            self.choice = self.view.show_menu()
            if self.choice == 1:
                self.play()
            if self.choice == 2:
                self.view.show_log()
            if self.choice == 3:
                self.clear_log()
            if self.choice == 4:
                break

    def play(self):
        """Plays game"""
        player1_name, player2_name = self.view.name_request()
        players = {0: Player(player1_name, self.view), 1: Player(player2_name, self.view)}
        game = False
        count = 2
        size = self.view.ask_size()
        self.model = Model(size)
        print(self.view.show_bord(self.model.board))
        wins = [0, 0]

        while not game:
            current_player = count % 2
            players[current_player].make_choice(self.model)
            logger.debug(
                f"{players[current_player].name}'s "
                f"turn {players[current_player].choice}")
            self.model.write_move(current_player,
                                  *players[current_player].choice)
            print(self.view.show_bord(self.model.board))
            game = self.model.check_win(current_player)
            if game:
                wins[current_player] += 1
                print(
                    f'Hey {players[current_player].name}, {game}')
                logger.debug(
                    f"{players[current_player].name}, {game}")
                logger.debug(
                    f"Count {players[0].name} : {players[1].name} /n {wins}")
                if input("Guys, do you wanna play more? 1(y/n)") == "y":
                    size = self.view.ask_size()
                    self.model = Model(size)
                    game = False
            count += 1


view = TerminalView()
controller = Controller(view)
controller.menu()
