"""
Suggestions taken from https://stackoverflow.com/a/287944
"""

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'


def colorify(text: str, *styles: str) -> str:
    def _parse(style: str) -> str:
        try:
            style = eval(style.upper())
        except NameError:
            raise ValueError(f'Invalid style "{style}". Must be one of '
                             'HEADER, OKBLUE, OKGREEN, WARNING, FAIL, BOLD, or UNDERLINE')
        else:
            if style == ENDC:
                raise ValueError('Invalid style "ENDC". Must be one of '
                                 'HEADER, OKBLUE, OKGREEN, WARNING, FAIL, BOLD, or UNDERLINE')
        return style

    styles = [_parse(style) for style in styles]
    return ''.join([*styles, text, ENDC])
