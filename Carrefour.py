import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
from datetime import datetime
import re

async def scrape_carrefour_cremosos():
    base_url = "https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Quesos/Quesos-cremosos-y-mozzarellas?order="
    all_products = []
    seen_product_names = set()
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    keywords = ["cremoso", "cremon"]

    async def scroll_para_cargar_suave(page):
        for i in range(10):
            await page.mouse.wheel(0, 800)
            await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(2)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(15000)

        current_page = 1
        max_pages = 10

        while current_page <= max_pages:
            url = f"{base_url}&page={current_page}"
            print(f"🔄 Visitando página {current_page}...")
            try:
                await page.goto(url, timeout=20000)
                await asyncio.sleep(3)
            except Exception as e:
                print(f"❌ Error cargando página {current_page}: {e}")
                break

            try:
                await page.wait_for_selector("div.valtech-carrefourar-search-result-3-x-gallery", timeout=15000)
            except:
                print("❌ Galería no encontrada")
                break

            await scroll_para_cargar_suave(page)

            products = page.locator("div.valtech-carrefourar-search-result-3-x-gallery > div > section > a")
            count = await products.count()
            if count == 0:
                break

            for i in range(count):
                product = products.nth(i)
                try:
                    # Nombre
                    name = "Nombre no disponible"
                    try:
                        name_element = product.locator("span.vtex-product-summary-2-x-productBrand")
                        if await name_element.count() > 0:
                            name = await name_element.inner_text(timeout=5000)
                            name = name.strip()
                    except:
                        pass

                    # Precio
                    price = "Precio no disponible"
                    try:
                        price_spans = product.locator("span.valtech-carrefourar-product-price-0-x-currencyContainer")
                        count_prices = await price_spans.count()
                        for j in range(count_prices):
                            span = price_spans.nth(j)
                            classes = await span.evaluate("e => e.className")
                            parent_classes = await span.evaluate("e => e.parentElement.className")
                            if "strikethrough" not in classes and "strikethrough" not in parent_classes:
                                price = await span.inner_text(timeout=3000)
                                price = price.strip()
                                break
                    except:
                        pass

                    if any(kw in name.lower() for kw in keywords) and name not in seen_product_names:
                        seen_product_names.add(name)
                        # Limpiar precio con reemplazos simples
                        precio_numero = price.replace(".", "").replace(",00", "").replace("$", "").strip()
                        all_products.append({
                            "fecha": fecha_actual,
                            "nombre": name,
                            "precio": precio_numero
                        })



                except:
                    continue

            await asyncio.sleep(2)
            current_page += 1

        await browser.close()
        return all_products

if __name__ == "__main__":
    resultados = asyncio.run(scrape_carrefour_cremosos())

    print(f"\n✅ Se encontraron {len(resultados)} productos con 'cremoso' o 'cremon'.")

    # Guardar en CSV
    if resultados:
        df = pd.DataFrame(resultados)
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        os.makedirs("Data/Raw", exist_ok=True)
        ruta_archivo = f"Data/Raw/carrefour_raw_{fecha_actual}.csv"
        df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
        print(f"📁 Archivo guardado en: {ruta_archivo}")
