import requests
import os
from bs4 import BeautifulSoup
import re
import pandas as pd

def main():

    try:
        req = requests.Session()
        output_list = []

        home_url = "https://books.toscrape.com/index.html"

        home_con_obj = req.get(home_url)
        print("CODE :: ", home_con_obj.status_code)

        home_con = home_con_obj.text

        home_soup = BeautifulSoup(home_con, "html.parser")

        if home_con_obj.status_code == 200:
            # with open("Cache/home_page.html", "w") as f:
            #     f.write(home_con)
            
            cat_container = home_soup.find("div", class_="side_categories")
            if cat_container:
                cat_links = cat_container.find_all("a")
                for cat_link in cat_links:
                    cat_name = cat_link.text.strip()
                    if cat_name.endswith("y"):
                        print("category :: ", cat_name)

                        cat_pg_link = cat_link.get("href")
                        cat_pg_link = "https://books.toscrape.com/" + cat_pg_link

                        get_list_content(req, cat_name, cat_pg_link, output_list)
                        # break
        else:
            print(f"{home_con_obj.status_code} error in homepage")
    except Exception as e:
        print("main error : ", e)
    finally:
        gen_output(output_list)


def get_list_content(req, cat_name, cat_pg_link, output_list):

    try:
        partial_link = cat_pg_link.split('index')[0]
        count = 1
        det_count = 1

        while True:
            print("cat_pg_link :: ", cat_pg_link)
            list_con_obj = req.get(cat_pg_link)
            print("CODE :: ", list_con_obj.status_code)

            if list_con_obj.status_code == 200:
                list_con = list_con_obj.text

                # with open(f"Cache/List_page_{cat_name}_page_{str(count)}.html", "w",encoding='utf-8') as f:
                #     f.write(list_con)
                count += 1

                list_soup = BeautifulSoup(list_con, "html.parser")

                blocks = list_soup.find_all("article", class_="product_pod")
                print("blocks :: ", len(blocks))
                for blk in blocks:
                    bk_link_obj = blk.h3.a
                    if bk_link_obj:
                        book_link = bk_link_obj.get("href")
                        book_link = re.sub(r"\.\.\/\.\.\/\.\.\/", "", book_link)
                        book_link = "https://books.toscrape.com/catalogue/" + book_link

                        # print("booklink :: ", book_link)

                        book_details = get_book_details(req, book_link, cat_name, det_count)
                        book_details['book_link'] = book_link
                        book_details['category_name'] = cat_name
                        det_count += 1
                        # print("book_details :: ", book_details)
                        if book_details:
                            output_list.append(book_details)

                
                next_page = list_soup.find("li", class_="next")
                if next_page:
                    next_page_link = next_page.a.get("href")
                    cat_pg_link = partial_link + next_page_link
                else:
                    break
            # break

    except Exception as e:
        print("get_list_content error : ", e)
        return None


def get_book_details(req, book_link, cat_name, det_count):

    try:
        # print("book_link :: ", book_link)
        book_con_obj = req.get(book_link)
        print("CODE :: ", book_con_obj.status_code)

        if book_con_obj.status_code == 200:
            book_con = book_con_obj.content

            # with open(f"Cache/Detail_page_{cat_name}_{str(det_count)}_details.html", "wb") as f:
            #     f.write(book_con)

            book_soup = BeautifulSoup(book_con, "html.parser")

            title=price=taxed_price=availability=availability_count=star_rating=description=upc=product_type=number_of_reviews=image_url=''

            title_obj = book_soup.h1
            if title_obj:
                title = title_obj.text.strip()
            
            price_obj = book_soup.find("p", class_="price_color")
            if price_obj:
                price = price_obj.text.strip().split('£')[1].strip()
            
            availability_obj = book_soup.find("p", class_="instock availability")
            if availability_obj:
                availability = availability_obj.text.strip().split('(')[0].strip()
                availability_count = availability_obj.text.strip().split('(')[1].strip().split('available')[0].strip()
            
            star_rating_obj = re.search(r"class=\"star-rating\s*([^>]*?)\"", str(book_con), flags=re.I)
            if star_rating_obj:
                star_rating = star_rating_obj.group(1).strip()
                if star_rating == "One":
                    star_rating = 1
                elif star_rating == "Two":
                    star_rating = 2
                elif star_rating == "Three":
                    star_rating = 3
                elif star_rating == "Four":
                    star_rating = 4
                elif star_rating == "Five":
                    star_rating = 5
            else:
                star_rating = None

            description_obj = book_soup.find(id="product_description")
            if description_obj:
                description = description_obj.find_next_sibling("p").text.strip()
            
            upc_obj = book_soup.find("th", string="UPC")
            if upc_obj:
                upc = upc_obj.find_next_sibling("td").text.strip()
            
            product_type_obj = book_soup.find("th", string="Product Type")
            if product_type_obj:
                product_type = product_type_obj.find_next_sibling("td").text.strip()
            
            number_of_reviews_obj = book_soup.find("th", string="Number of reviews")
            if number_of_reviews_obj:
                number_of_reviews = number_of_reviews_obj.find_next_sibling("td").text.strip()
            
            image_url_obj = book_soup.find("img")
            if image_url_obj:
                image_url = image_url_obj.get("src")
                image_url = re.sub(r"\.\.\/\.\.\/", "", image_url)
                image_url = "https://books.toscrape.com/" + image_url
            


            return {
                "title": title,
                "price": price,
                "availability": availability,
                "availability_count": availability_count,
                "star_rating": star_rating,
                "description": description,
                "upc": upc,
                "product_type": product_type,
                "number_of_reviews": number_of_reviews,
                "image_url": image_url,
                "book_link": book_link,
                "category_name": cat_name
            }


    except Exception as e:
        print("get_book_details error : ", e)

def gen_output(output_list):
    try:
        if output_list:
            df = pd.DataFrame(output_list)

            df['price'] = df['price'].astype(float)
            df['availability_count'] = df['availability_count'].astype(int)
            df['number_of_reviews'] = df['number_of_reviews'].astype(int)
            df['star_rating'] = df['star_rating'].astype(float)
            print(df.dtypes)


            def calculate_tax(price):
                if price > 50:
                    return round(price * 0.06, 2)
                elif price > 30:
                    return round(price * 0.03, 2)
                else:
                    return 0.0


            df['tax'] = df['price'].apply(calculate_tax)
            df['price_with_tax'] = df['price'] + df['tax']

            df.columns = ["Title", "Price", "Availability", "Availability Count", "Star Rating", "Description", "UPC", "Product Type", "Number of Reviews", "Image URL", "Book Link", "Category Name", "Tax", "Price with Tax"]

            df = df[["Title", "Description", "Category Name", "Star Rating", "Price", "Tax", "Price with Tax", "Availability", "Availability Count", "UPC", "Product Type", "Number of Reviews", "Image URL", "Book Link"]]

            df.to_csv("Output.csv", index=False, encoding='utf-8')

    except Exception as e:
        print("gen_output error : ", e)
