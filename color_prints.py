class Colors:
    """Класс для цветного вывода в консоль"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'


def print_success(message):
    """Вывод успешного сообщения"""
    print(f"{Colors.GREEN}✅ {message}{Colors.RESET}")


def print_error(message):
    """Вывод сообщения об ошибке"""
    print(f"{Colors.RED}❌ {message}{Colors.RESET}")


def print_warning(message):
    """Вывод предупреждения"""
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.RESET}")


def print_info(message):
    """Вывод информационного сообщения"""
    print(f"{Colors.BLUE}ℹ️  {message}{Colors.RESET}")