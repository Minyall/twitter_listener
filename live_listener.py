import argparse
from functions import build_logger
from listener_hub import Core_Listener
from my_credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_SECRET, ACCESS_KEY




if __name__ == '__main__':

    main_logger = build_logger('main_logger','main.log')
    parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
    parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
    parser.add_argument('-q', '--query', action='store', nargs='+', required=True)
    parser.add_argument('-r', '--retweets', help='Boolean: Collect Retweets - Default False',
                        action='store_true', default=False)
    parser.add_argument('-v', '--verbosity', action='store', required=False, type=int, default=60,
                        help='Integer - Duration between logging updates in seconds')
    parser.add_argument('-s', '--show_sample', action='store_true', required=False, default=False, help='Switch: Include argument'
                        ' to have logging return a sample of status texts')
    parser.add_argument('-sn', '--sample_n', action='store', type=int, default=5, required=False, help=
                        "If using --show_sample, sets the number of sample items to show.")
    parser.add_argument('-sl', '--sample_len', action='store', type=int, default=100, required=False, help=
                        "Character limit of displayed samples - good for ensuring a clean fit on the screen. Default 100.")


    try:
        args = vars(parser.parse_args())
        listener = Core_Listener(args,
                                 CONSUMER_KEY,
                                 CONSUMER_SECRET,
                                 ACCESS_KEY,
                                 ACCESS_SECRET,
                                 logger=main_logger)

        listener.initiate_core()

    except KeyboardInterrupt:
        main_logger.info('Shutdown signal recieved...')
        main_logger.info('Shutdown Complete. Have a nice day!')