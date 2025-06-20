from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import json
import time
import logging
import os
import sys
from datetime import datetime
import re
import argparse
import random
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib3.util.retry import Retry
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

class TollBrothersDetailScraper:
    """Toll Brothers小区详情页爬虫"""
    
    def __init__(self):
        """初始化爬虫"""
        self.driver = None
        self.max_retries = 3
        self.retry_delay = 5
        self.base_url = "https://www.tollbrothers.com"
        
    def _setup_driver(self):
        """设置Chrome浏览器"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--force-device-scale-factor=1')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.page_load_strategy = 'eager'
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            self.driver.set_script_timeout(30)
            self.driver.set_window_size(1920, 1080)
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
                time.sleep(10)  # 增加初始等待时间
                
                if wait_class:
                    try:
                        # 增加等待时间到30秒
                        WebDriverWait(self.driver, 30).until(
                            EC.presence_of_element_located((By.CLASS_NAME, wait_class))
                        )
                    except TimeoutException:
                        logger.warning(f"等待元素 {wait_class} 超时，尝试使用备用选择器")
                        # 尝试使用备用选择器
                        try:
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, 'body'))
                            )
                        except TimeoutException:
                            logger.warning("备用选择器也超时，继续处理页面")
                
                try:
                    # 增加页面加载完成等待时间
                    WebDriverWait(self.driver, 30).until(
                        lambda driver: driver.execute_script('return document.readyState') == 'complete'
                    )
                except TimeoutException:
                    logger.warning("页面加载超时，继续处理当前内容")
                
                # 添加额外的等待时间，确保动态内容加载完成
                time.sleep(5)
                
                page_source = self.driver.page_source
                if not page_source:
                    raise Exception("页面内容为空")
                    
                soup = BeautifulSoup(page_source, 'html.parser')
                if not soup:
                    raise Exception("无法解析页面内容")
                
                # 保存页面源码用于调试
                debug_dir = 'debug'
                os.makedirs(debug_dir, exist_ok=True)
                with open(os.path.join(debug_dir, 'page_source.html'), 'w', encoding='utf-8') as f:
                    f.write(page_source)
                logger.info("已保存页面源码用于调试")
                    
                return soup
                
            except Exception as e:
                logger.error(f"第 {attempt + 1} 次尝试失败: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # 递增重试延迟
                    try:
                        self.driver.quit()
                    except:
                        pass
                    self.driver = None
                else:
                    logger.error(f"达到最大重试次数，放弃获取页面: {url}")
                    return None
                    
        return None
        
    def extract_price_range(self, soup):
        """提取价格范围"""
        try:
            # 查找价格信息
            price_text = soup.find(text=re.compile(r'starting at \$[\d,]+'))
            if price_text:
                return price_text.strip()
            return None
        except Exception as e:
            logger.error(f"提取价格范围时出错: {e}")
            return None

    def extract_beds_baths_range(self, soup):
        """提取卧室和卫生间范围"""
        try:
            # 从户型信息中提取
            home_designs = soup.find_all('div', class_=lambda x: x and 'home-design' in x.lower())
            beds = set()
            baths = set()
            
            for design in home_designs:
                # 查找包含卧室和卫生间信息的元素
                specs = design.find_all(text=re.compile(r'\d+\s*(?:bd|ba)'))
                for spec in specs:
                    if 'bd' in spec:
                        beds.add(spec.strip())
                    elif 'ba' in spec:
                        baths.add(spec.strip())
            
            beds_range = f"{min(beds)} - {max(beds)}" if beds else None
            baths_range = f"{min(baths)} - {max(baths)}" if baths else None
            
            return beds_range, baths_range
        except Exception as e:
            logger.error(f"提取卧室和卫生间范围时出错: {e}")
            return None, None

    def extract_sqft_range(self, soup):
        """提取面积范围"""
        try:
            # 从户型信息中提取
            home_designs = soup.find_all('div', class_=lambda x: x and 'home-design' in x.lower())
            sqft_values = set()
            
            for design in home_designs:
                # 查找包含面积信息的元素
                sqft_text = design.find(text=re.compile(r'\d{1,3}(?:,\d{3})*\s*sqft'))
                if sqft_text:
                    sqft = re.search(r'(\d{1,3}(?:,\d{3})*)', sqft_text)
                    if sqft:
                        sqft_values.add(int(sqft.group(1).replace(',', '')))
            
            if sqft_values:
                min_sqft = min(sqft_values)
                max_sqft = max(sqft_values)
                return f"{min_sqft:,} - {max_sqft:,}"
            return None
        except Exception as e:
            logger.error(f"提取面积范围时出错: {e}")
            return None

    def extract_amenities(self, soup):
        """提取设施信息"""
        try:
            amenities = []
            # 查找设施部分
            amenities_section = soup.find('section', string=re.compile('Elevate the Everyday'))
            if amenities_section:
                amenity_items = amenities_section.find_all('div', class_=lambda x: x and 'amenity' in x.lower())
                for item in amenity_items:
                    name = item.find('h3')
                    desc = item.find('p')
                    if name:
                        amenities.append({
                            "name": name.text.strip(),
                            "description": desc.text.strip() if desc else "",
                            "icon_url": ""
                        })
            return amenities
        except Exception as e:
            logger.error(f"提取设施信息时出错: {e}")
            return []

    def extract_phone(self, soup):
        """提取电话信息"""
        try:
            phone_element = soup.find(text=re.compile(r'\d{3}-\d{3}-\d{4}'))
            if phone_element:
                return phone_element.strip()
            return ""
        except Exception as e:
            logger.error(f"提取电话信息时出错: {e}")
            return ""

    def extract_address(self, soup):
        """提取地址信息"""
        try:
            address_element = soup.find('div', string=re.compile(r'Goodyear, AZ \d{5}'))
            if address_element:
                return address_element.text.strip()
            return ""
        except Exception as e:
            logger.error(f"提取地址信息时出错: {e}")
            return ""

    def extract_description(self, soup):
        """提取描述信息"""
        try:
            desc_element = soup.find('div', string=re.compile('Located in an amenity-rich master-planned community'))
            if desc_element:
                return desc_element.text.strip()
            return ""
        except Exception as e:
            logger.error(f"提取描述信息时出错: {e}")
            return ""

    def extract_location(self, soup, address=None):
        """提取位置信息"""
        try:
            location = {
                "latitude": None,
                "longitude": None,
                "address": {
                    "city": None,
                    "state": None,
                    "market": None
                }
            }
            
            if address:
                address_parts = address.split(',')
                if len(address_parts) > 1:
                    location["address"]["city"] = address_parts[1].strip()
                if len(address_parts) > 2:
                    location["address"]["state"] = address_parts[2].strip()
            
            location_script = soup.find('script', string=re.compile('latitude'))
            if location_script:
                lat_match = re.search(r'latitude["\s:]+([\d.-]+)', location_script.string)
                lng_match = re.search(r'longitude["\s:]+([\d.-]+)', location_script.string)
                if lat_match and lng_match:
                    location["latitude"] = float(lat_match.group(1))
                    location["longitude"] = float(lng_match.group(1))
            
            return location
        except Exception as e:
            logger.error(f"提取位置信息时出错: {e}")
            return {
                "latitude": None,
                "longitude": None,
                "address": {
                    "city": None,
                    "state": None,
                    "market": None
                }
            }

    def extract_images(self, soup):
        """提取图片信息
        只从id为toScroll-gallery的元素中获取图片
        """
        try:
            images = []
            # 查找id为toScroll-gallery的元素
            gallery = soup.find(id='toScroll-gallery')
            if gallery:
                # 在gallery中查找所有img标签
                image_elements = gallery.find_all('img')
                for img in image_elements:
                    if img.get('src') and not img['src'].startswith('data:'):
                        images.append(img['src'])
            return images
        except Exception as e:
            logger.error(f"提取图片信息时出错: {e}")
            return []

    def extract_homeplans(self, soup):
        """提取户型信息"""
        try:
            homeplans = []
            # 查找户型部分
            home_designs = soup.find_all('div', class_=lambda x: x and 'home-design' in x.lower())
            for design in home_designs:
                name = design.find('h3')
                details = design.find_all(text=re.compile(r'\d+\s*(?:bd|ba|sqft)'))
                if name and details:
                    homeplan = {
                        "name": name.text.strip(),
                        "url": "",  # 需要从链接中提取
                        "details": {
                            "price": "",
                            "beds": next((d for d in details if 'bd' in d), ""),
                            "baths": next((d for d in details if 'ba' in d), ""),
                            "sqft": next((d for d in details if 'sqft' in d), ""),
                            "status": "",
                            "image_url": ""
                        },
                        "includedFeatures": []
                    }
                    homeplans.append(homeplan)
            return homeplans
        except Exception as e:
            logger.error(f"提取户型信息时出错: {e}")
            return []

    def extract_jsonld_data(self, soup):
        """从JSON-LD中提取关键信息"""
        jsonld_data = {}
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                # 只处理字典类型
                if isinstance(data, dict):
                    # 住宅信息
                    if data.get('@type') in ['SingleFamilyResidence', 'Residence', 'WebPage', 'Place', 'Organization']:
                        jsonld_data.update(data)
            except Exception as e:
                continue
        return jsonld_data

    def extract_homesites(self, soup):
        """从Quick Move-In Homes模块提取房源信息，列表页采集基础字段，详情页补充采集特殊字段"""
        homesites = []
        try:
            # 精确定位所有房源卡片
            cards = soup.select('div.modelCardWrap__adjust.ModelCard_modelCardContainer__lXz5R')
            logger.info(f"找到 {len(cards)} 个房源卡片")
            for card in cards:
                try:
                    # 列表页基础字段
                    a = card.select_one('a.ModelCard_modelCardContainer__lXz5R')
                    url = a['href'] if a and a.has_attr('href') else None
                    if not url:
                        logger.warning("未找到房源链接")
                        continue
                    if not url.startswith('http'):
                        url = f"https://www.tollbrothers.com{url}"

                    img = card.select_one('img.BlurBackgroundFill_modelCardImg__fpCCc')
                    image_url = img['src'] if img and img.has_attr('src') else None

                    name = card.select_one('h4.ModelCard_modelName__XzUo2')
                    name = name.text.strip() if name else None

                    price = card.select_one('p.ModelCard_modelPrice__oqOXq')
                    price = price.text.strip() if price else None

                    beds = card.select_one('p.tracking_bedRange')
                    beds = beds.text.strip() if beds else None

                    baths = card.select_one('p.tracking_bathRange')
                    baths = baths.text.strip() if baths else None

                    sqft = card.select_one('p.tracking_sqftRange')
                    sqft = sqft.text.strip().replace(',', '') if sqft else None

                    status = card.select_one('div.ModelCard_modelCardCallout__MdHUW')
                    status = status.text.strip() if status else None

                    overview = None  # 列表页无描述

                    # 详情页补充字段
                    latitude = None
                    longitude = None
                    description = None
                    images = [image_url] if image_url else []
                    plan = None
                    address = None
                    id = None

                    # 进入详情页采集特殊字段
                    logger.info(f"正在访问房源详情页: {url}")
                    detail_soup = self._safe_get_page(url, 'body')
                    if detail_soup:
                        jsonld = self.extract_jsonld_data(detail_soup)
                        if jsonld:
                            # 经纬度
                            if 'geo' in jsonld and isinstance(jsonld['geo'], dict):
                                latitude = jsonld['geo'].get('latitude', None)
                                longitude = jsonld['geo'].get('longitude', None)
                            # 详细描述
                            description = jsonld.get('description', None)
                            # 地址
                            if 'address' in jsonld and isinstance(jsonld['address'], dict):
                                addr = jsonld['address']
                                address = f"{addr.get('streetAddress', '')}, {addr.get('addressLocality', '')}, {addr.get('addressRegion', '')} {addr.get('postalCode', '')}".strip(', ')
                            # 图片
                            if 'image' in jsonld:
                                if isinstance(jsonld['image'], list):
                                    images = jsonld['image']
                                elif isinstance(jsonld['image'], str):
                                    images = [jsonld['image']]
                            # id
                            if 'url' in jsonld:
                                id_match = re.search(r'/(\w+)$', jsonld['url'])
                                if id_match:
                                    id = id_match.group(1)
                        # 详情页补充plan
                        plan_elem = detail_soup.select_one('h1, h2, h3')
                        if plan_elem:
                            plan = plan_elem.text.strip()
                        # 详情页补充更多图片
                        if not images or len(images) < 2:
                            img_elems = detail_soup.find_all('img')
                            for img in img_elems:
                                if img.get('src') and not img['src'].startswith('data:') and img['src'] not in images:
                                    images.append(img['src'])
                    else:
                        logger.warning(f"无法访问房源详情页: {url}")

                    # 构建房源信息
                    homesite = {
                        "name": name,
                        "plan": plan,
                        "id": id,
                        "address": address,
                        "price": price,
                        "beds": beds,
                        "baths": int(baths) if baths and baths.isdigit() else None,
                        "sqft": sqft,
                        "status": status,
                        "image_url": images[0] if images else None,
                        "url": url,
                        "latitude": latitude,
                        "longitude": longitude,
                        "overview": description,
                        "images": images
                    }
                    homesites.append(homesite)
                except Exception as e:
                    logger.error(f"提取房源卡片失败: {e}")
                    continue
        except Exception as e:
            logger.error(f"提取Quick Move-In Homes模块失败: {e}")
        return homesites

    def get_community_details(self, community_url):
        """获取小区详细信息，所有字段都补全，结构严格对齐example.json"""
        try:
            selectors = [
                'SearchProductCard_card__htFY3',
                'SearchProductCard',
                'community-details',
                'product-card',
                'body'
            ]
            soup = None
            for selector in selectors:
                logger.info(f"尝试使用选择器: {selector}")
                soup = self._safe_get_page(community_url, selector)
                if soup:
                    logger.info(f"成功使用选择器: {selector}")
                    break
            if not soup:
                logger.error(f"无法获取页面内容: {community_url}")
                return None

            # 1. 先从JSON-LD提取基础信息
            jsonld = self.extract_jsonld_data(soup)
            # 2. 提取基础字段
            community_name = jsonld.get('name', '')
            address = ''
            if 'address' in jsonld and isinstance(jsonld['address'], dict):
                addr = jsonld['address']
                address = f"{addr.get('streetAddress', '')}, {addr.get('addressLocality', '')}, {addr.get('addressRegion', '')} {addr.get('postalCode', '')}".strip(', ')
            phone = jsonld.get('telephone', '')
            
            # 优先从指定class获取description
            description_elem = soup.find('p', class_='CommunityOverview_overviewDescription__0bJS6 tracking_prop_body')
            description = description_elem.text.strip() if description_elem else (jsonld.get('description', '') or '')
            
            price = jsonld.get('priceRange', None) or jsonld.get('offers', {}).get('price', None)
            # 3. 提取地理坐标
            latitude = None
            longitude = None
            if 'geo' in jsonld and isinstance(jsonld['geo'], dict):
                latitude = jsonld['geo'].get('latitude', None)
                longitude = jsonld['geo'].get('longitude', None)
            location = {
                "latitude": latitude,
                "longitude": longitude,
                "address": {
                    "city": jsonld.get('address', {}).get('addressLocality', None) if isinstance(jsonld.get('address', {}), dict) else None,
                    "state": jsonld.get('address', {}).get('addressRegion', None) if isinstance(jsonld.get('address', {}), dict) else None,
                    "market": None
                }
            }
            # 4. 提取图片
            images = []
            if 'image' in jsonld:
                if isinstance(jsonld['image'], list):
                    images = jsonld['image']
                elif isinstance(jsonld['image'], str):
                    images = [jsonld['image']]
            if not images:
                images = self.extract_images(soup)
            # 5. 其他字段
            status = None
            beds_range, baths_range = self.extract_beds_baths_range(soup)
            sqft_range = self.extract_sqft_range(soup)
            stories_range = ""
            community_count = 0
            # 6. amenities、homeplans
            amenities = self.extract_amenities(soup)
            homeplans = self.extract_homeplans(soup)
            # 7. homesites、nearbyplaces
            homesites = self.extract_homesites(soup)
            nearbyplaces = []
            # 8. 结构严格对齐example.json
            community_info = {
                "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'),
                "name": community_name or '',
                "url": community_url,
                "status": status,
                "price_from": price,
                "address": address,
                "phone": phone,
                "description": description,
                "images": images,
                "location": location,
                "details": {
                    "price_range": price,
                    "sqft_range": sqft_range,
                    "bed_range": beds_range,
                    "bath_range": baths_range,
                    "stories_range": stories_range,
                    "community_count": community_count
                },
                "amenities": amenities if amenities else [],
                "homeplans": homeplans if homeplans else [],
                "homesites": homesites,
                "nearbyplaces": nearbyplaces
            }
            logger.info(f"成功获取小区信息: {community_name}")
            return community_info
        except Exception as e:
            logger.error(f"获取小区详情失败: {e}")
            return None
            
    def save_data(self, data, filename):
        """保存数据到JSON文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"数据已保存到: {filename}")
        except Exception as e:
            logger.error(f"保存数据失败: {e}")

    def save_html(self, html_content, filename):
        """保存HTML内容到文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"HTML已保存到: {filename}")
        except Exception as e:
            logger.error(f"保存HTML失败: {e}")

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
        # 创建命令行参数解析器
        parser = argparse.ArgumentParser(description='爬取Toll Brothers社区页面')
        parser.add_argument('--url', help='处理单个URL')
        parser.add_argument('--batch', action='store_true', help='处理tollbrothers_links.json中的所有URL')
        args = parser.parse_args()

        # 确保输出目录存在
        output_dir = 'data/tollbrothers'
        os.makedirs(f'{output_dir}/html', exist_ok=True)
        os.makedirs(f'{output_dir}/json', exist_ok=True)
        
        # 初始化爬虫
        scraper = TollBrothersDetailScraper()
        
        try:
            if args.url:
                # 处理单个URL
                logger.info(f"正在处理URL: {args.url}")
                community_info = scraper.get_community_details(args.url)
                if community_info:
                    # 保存JSON数据
                    filename = os.path.basename(args.url).replace('/', '_')
                    json_path = os.path.join(output_dir, 'json', f'{filename}.json')
                    scraper.save_data(community_info, json_path)
                    
                    # 保存HTML内容
                    if scraper.driver:
                        html_path = os.path.join(output_dir, 'html', f'{filename}.html')
                        scraper.save_html(scraper.driver.page_source, html_path)
                
            elif args.batch:
                # 读取社区链接
                try:
                    with open('output/communities_links.json', 'r', encoding='utf-8') as f:
                        community_links = json.load(f)
                except Exception as e:
                    logger.error(f"读取社区链接文件失败: {e}")
                    return
                    
                if not community_links:
                    logger.error("未找到社区链接")
                    return
                    
                # 获取所有社区详情
                total_communities = len(community_links)
                success_count = 0
                
                for index, community in enumerate(community_links, 1):
                    try:
                        logger.info(f"正在处理第 {index}/{total_communities} 个社区")
                        community_info = scraper.get_community_details(community['url'])
                        if community_info:
                            # 保存单个社区的JSON数据
                            filename = os.path.basename(community['url']).replace('/', '_')
                            json_path = os.path.join(output_dir, 'json', f'{filename}.json')
                            scraper.save_data(community_info, json_path)
                            
                            # 保存HTML内容
                            if scraper.driver:
                                html_path = os.path.join(output_dir, 'html', f'{filename}.html')
                                scraper.save_html(scraper.driver.page_source, html_path)
                            
                            logger.info(f"成功获取社区信息: {community_info['name']}")
                            success_count += 1
                        else:
                            logger.warning(f"未获取到社区信息: {community['url']}")
                            
                        # 添加随机延迟
                        time.sleep(random.uniform(2, 5))
                        
                    except Exception as e:
                        logger.error(f"处理社区时出错: {e}")
                        continue
                
                logger.info(f"处理完成，成功获取 {success_count}/{total_communities} 个社区信息")
            else:
                logger.error("请指定 --url 或 --batch 参数")
                
        finally:
            # 确保关闭浏览器
            scraper.close()
            
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        
if __name__ == "__main__":
    main() 