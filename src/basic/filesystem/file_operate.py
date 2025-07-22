from pathlib import Path
import re
from src.basic.config import settings

class FileSystemTool:
    """文件系统工具类"""

    def __init__(self):
        """
        初始化文件系统工具
        :param root_dir: 根目录
        """
        self.root_dir = Path(settings.LOCAL_REPO_PATH)

    def read_file(self, file_path):
        """
        读取文件内容
        :param file_path: 文件路径（相对根目录）
        :return: 文件内容
        """
        file_path = self.root_dir / file_path
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            print(f"读取文件失败：{e}")
            return None

    def update_file(self, file_path, content):
        """
        更新文件内容
        :param file_path: 文件路径（相对根目录）
        :param content: 要写入的内容
        :return: 是否成功
        """
        file_path = self.root_dir / file_path
        try:
            # 如果文件不存在，则创建文件和必要的目录
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content)
            print(f"文件更新成功：{file_path}")
            return True
        except Exception as e:
            print(f"更新文件失败：{e}")
            return False

    def search_files_by_name(self, pattern):
        """
        按文件名搜索文件
        :param pattern: 文件名模式（支持通配符 * 和 ?）
        :return: 匹配的文件列表
        """
        try:
            pattern = re.escape(pattern).replace(r'\*', '.*').replace(r'\?', '.')
            regex = re.compile(f'.*{pattern}', re.IGNORECASE)
            matched_files = []
            for file_path in self.root_dir.rglob('nb_*'):
                if file_path.is_file() and regex.match(file_path.stem):
                    matched_files.append(file_path.relative_to(self.root_dir))
            return matched_files
        except Exception as e:
            print(f"搜索文件失败：{e}")
            return []

    def search_files_by_content(self, keyword):
        """
        按文件内容搜索文件
        :param keyword: 要搜索的关键词
        :return: 匹配的文件列表及其内容片段
        """
        try:
            matched_files = []
            for file_path in self.root_dir.rglob('*'):
                if file_path.is_file():
                    try:
                        with open(file_path, 'r', encoding='utf-8') as file:
                            content = file.read()
                            if keyword in content:
                                matched_files.append(
                                    (str(file_path.relative_to(self.root_dir)),
                                     content[:50] + '...' if len(content) > 50 else content))
                    except Exception as e:
                        # 如果文件无法读取（如二进制文件），跳过
                        continue
            return matched_files
        except Exception as e:
            print(f"搜索文件失败：{e}")
            return []
