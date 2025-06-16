import os
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TollBrothersScraper:
    """Toll Brothers 网站数据采集器"""
    
    def __init__(self):
        """初始化采集器"""
        self.base_url = "https://www.tollbrothers.com"
        self.driver = None
        self.session = self._setup_session()
        self.all_community_links = []  # 存储所有社区链接
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 5  # 重试延迟（秒）

    def _setup_session(self):
        """设置请求会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': self.base_url
        })
        return session

    def _setup_driver(self):
        """设置Chrome浏览器"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')  # 禁用GPU
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-software-rasterizer')  # 禁用软件光栅化
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')  # 只显示致命错误
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--force-device-scale-factor=1')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            
            # 添加实验性选项
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 设置页面加载策略
            chrome_options.page_load_strategy = 'eager'
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            self.driver.set_script_timeout(30)
            
            # 设置窗口大小
            self.driver.set_window_size(1920, 1080)
            
            # 执行 JavaScript 来修改 webdriver 标记
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return True
        except Exception as e:
            logger.error(f"设置Chrome浏览器失败: {e}")
            return False

    def _safe_get_page(self, url, wait_class=None):
        """安全地获取页面内容"""
        for attempt in range(self.max_retries):
            try:
                if not self.driver:
                    if not self._setup_driver():
                        logger.error("无法初始化浏览器")
                        return None
                    
                logger.info(f"正在访问页面: {url}")
                self.driver.get(url)
                
                # 等待页面加载完成
                time.sleep(5)  # 基础等待时间
                
                # 等待特定元素出现
                if wait_class:
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, wait_class))
                        )
                    except TimeoutException:
                        logger.warning(f"等待元素 {wait_class} 超时")
                
                # 等待页面完全加载
                try:
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: driver.execute_script('return document.readyState') == 'complete'
                    )
                except TimeoutException:
                    logger.warning("页面加载超时")
                
                # 获取页面内容
                page_source = self.driver.page_source
                if not page_source:
                    raise Exception("页面内容为空")
                    
                soup = BeautifulSoup(page_source, 'html.parser')
                if not soup:
                    raise Exception("无法解析页面内容")
                    
                return soup
                
            except Exception as e:
                logger.error(f"第 {attempt + 1} 次尝试失败: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    try:
                        self.driver.quit()
                    except:
                        pass
                    self.driver = None
                else:
                    logger.error(f"达到最大重试次数，放弃获取页面: {url}")
                    return None
                
        return None

    def get_locations(self):
        """获取所有州和城市信息"""
        try:
            soup = self._safe_get_page(self.base_url, 'MetroGrid_metro_areas_states___Ox83')
            if not soup:
                return []

            locations = []
            # 查找所有州区域
            state_sections = soup.find_all('li', class_='MetroGrid_metro_areas_states___Ox83')
            
            for state_section in state_sections:
                try:
                    # 获取州名
                    state_name = state_section.find('h3').text.strip() if state_section.find('h3') else ''
                    
                    # 获取该州下的所有城市
                    city_links = state_section.find_all('a', href=True)
                    for city_link in city_links:
                        city_url = city_link['href']
                        if not city_url.startswith('http'):
                            city_url = f"https://www.tollbrothers.com{city_url}"
                            
                        city_info = {
                            'state': state_name,
                            'name': city_link.text.strip(),
                            'url': city_url
                        }
                        locations.append(city_info)
                        logger.info(f"获取到城市信息: {state_name} - {city_info['name']}")
                        
                except Exception as e:
                    logger.error(f"处理州 {state_name} 时出错: {e}")
                    continue
                
            return locations
            
        except Exception as e:
            logger.error(f"获取州和城市信息失败: {e}")
            return []

    def get_communities(self, city_url):
        """获取城市下的所有社区信息"""
        community_links = []
        try:
            soup = self._safe_get_page(city_url, 'SearchProductCard_card__htFY3')
            if not soup:
                return community_links

            # 查找所有社区卡片
            community_cards = soup.find_all('div', class_='SearchProductCard_cardWrap__2CFt9')
            
            for card in community_cards:
                try:
                    # 获取社区链接
                    link_element = card.find('a', href=True)
                    if not link_element:
                        continue
                        
                    community_url = link_element['href']
                    if not community_url.startswith('http'):
                        community_url = f"https://www.tollbrothers.com{community_url}"
                    
                    # 获取社区名称
                    name_element = card.find('h2', class_='SearchProductCard_card_header__F_ORx')
                    community_name = name_element.text.strip() if name_element else ''
                    
                    # 获取位置信息
                    location_element = card.find('div', class_='SearchProductCard_location_description__7kNyd')
                    location = location_element.text.strip() if location_element else ''
                    
                    # 获取价格信息
                    price_element = card.find('div', class_='ProductPrice_product_price__VbtDE')
                    price = price_element.find('div').text.strip() if price_element and price_element.find('div') else ''
                    
                    # 获取房屋详情
                    details = {}
                    detail_items = card.find_all('li', class_='SearchProductDetail_product_detail__q9eCj')
                    for item in detail_items:
                        detail_text = item.find('span', class_='detail')
                        if detail_text:
                            label = item.find('img')['alt'].lower().replace(' icon', '')
                            details[label] = detail_text.text.strip()
                    
                    # 获取社区类型
                    community_type = ''
                    type_element = card.find('span', class_='commTypes__js')
                    if type_element:
                        community_type = type_element.text.strip()
                    
                    # 获取房屋类型
                    home_type = ''
                    home_type_element = card.find('span', class_='homeTypes__js')
                    if home_type_element:
                        home_type = home_type_element.text.strip()
                    
                    community_info = {
                        'url': community_url,
                        'name': community_name,
                        'location': location,
                        'price': price,
                        'details': details,
                        'community_type': community_type,
                        'home_type': home_type
                    }
                    
                    community_links.append(community_info)
                    logger.info(f"获取到社区信息: {community_name}")
                    
                except Exception as e:
                    logger.error(f"处理社区卡片时出错: {e}")
                    continue
                
            return community_links
            
        except Exception as e:
            logger.error(f"获取社区信息时出错: {e}")
            return community_links

    def save_data(self, data, filename):
        """保存数据到文件"""
        try:
            os.makedirs('output', exist_ok=True)
            filepath = os.path.join('output', filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"数据已保存到: {filepath}")
            
        except Exception as e:
            logger.error(f"保存数据失败: {e}")

    def close(self):
        """关闭浏览器"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

def main():
    """主函数"""
    try:
        # 创建输出目录
        os.makedirs('output', exist_ok=True)
        
        # 初始化爬虫
        scraper = TollBrothersScraper()
        
        # 获取所有州和城市信息
        locations = scraper.get_locations()
        if not locations:
            logger.error("获取州和城市信息失败，程序退出")
            return
            
        # 获取所有社区链接
        all_community_links = []
        total_cities = len(locations)
        
        for index, city in enumerate(locations, 1):
            try:
                logger.info(f"正在处理第 {index}/{total_cities} 个城市: {city['name']}")
                community_links = scraper.get_communities(city['url'])
                if community_links:
                    all_community_links.extend(community_links)
                    logger.info(f"成功获取 {len(community_links)} 个社区信息")
                else:
                    logger.warning(f"未获取到城市 {city['name']} 的社区信息")
                    
                # 添加随机延迟，避免请求过快
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"处理城市 {city['name']} 时出错: {e}")
                continue
                
        # 保存数据
        if all_community_links:
            scraper.save_data(all_community_links, 'communities_links.json')
            logger.info(f"成功保存 {len(all_community_links)} 个社区信息")
        else:
            logger.error("未获取到任何社区链接")
            
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
    finally:
        # 确保关闭浏览器
        if 'scraper' in locals():
            scraper.close()
            
if __name__ == "__main__":
    main() 