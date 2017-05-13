class Initialize:

    def __init__(self):
        self.user = ''

    def login(self):
        while 1:
            login_name = input('Username: ')
            if user == '':
                print('Please enter a Username to begin.')
            else:
                print('Starting Environment Reporting Program...')
                break

        self.user = user

if __name__ == '__main__':
    prog = Initialize()
    prog.login()
    input('\n\nPress enter to exit')
