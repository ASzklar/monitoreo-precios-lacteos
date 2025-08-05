import re
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
from datetime import datetime

async def scrape_coope_cremoso(max_pages=5):
    url = "https://www.lacoopeencasa.coop/"
    busqueda = "queso cremoso"
    patron = re.compile(r"cremoso|cremon", re.IGNORECASE)
    productos = []
    fecha_actual = datetime.now().strftime('%Y-%m-%d')

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        await page.wait_for_selector("input#idInputBusqueda")

        # Buscar queso cremoso
        await page.fill("input#idInputBusqueda", busqueda)
        await page.keyboard.press("Enter")
        await page.wait_for_selector("div.card-content", timeout=40000)

        for _ in range(max_pages):
            # Scroll para cargar productos
            previous_height = 0
            while True:
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)
                current_height = await page.evaluate("document.body.scrollHeight")
                if current_height == previous_height:
                    break
                previous_height = current_height

            # Extraer productos
            cards = await page.query_selector_all("div.card-content")
            for card in cards:
                nombre_elem = await card.query_selector("div.card-descripcion p.text-capitalize")
                nombre = await nombre_elem.inner_text() if nombre_elem else ""
                nombre = nombre.strip()

                # Filtrar solo productos cremosos
                if not patron.search(nombre):
                    continue

                precio_entero = await card.query_selector("div.precio-entero")
                precio_decimal = await card.query_selector("div.precio-decimal")

                precio_text = ""
                if precio_entero:
                    precio_text = (await precio_entero.inner_text()).strip()

                if precio_decimal:
                    precio_text += "," + (await precio_decimal.inner_text()).strip()
                else:
                    precio_text += ",00"

                # Quitar $ y espacios, solo el número
                precio_numero = precio_text.replace("$", "").replace(" ", "").replace(".", "").replace(",00", "")

                productos.append({
                    "fecha": fecha_actual,
                    "nombre": nombre,
                    "precio": precio_numero
                })

            # Ir a la siguiente página si existe
            btn_siguiente = await page.query_selector("ul.pagination li.waves-effect svg use[href*='derecha']")
            if btn_siguiente:
                btn_siguiente_parent = await btn_siguiente.evaluate_handle("node => node.closest('li')")
                if btn_siguiente_parent:
                    await btn_siguiente_parent.click()
                    await page.wait_for_timeout(4000)
                    await page.wait_for_selector("div.card-content", timeout=40000)
                else:
                    break
            else:
                break

        await browser.close()
        return productos

if __name__ == "__main__":
    resultados = asyncio.run(scrape_coope_cremoso())
    print(f"\n✅ Se encontraron {len(resultados)} productos de 'queso cremoso' en La Coope.")

    if resultados:
        df = pd.DataFrame(resultados)
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        os.makedirs("Data/Raw", exist_ok=True)
        ruta_archivo = f"Data/Raw/coope_raw_{fecha_actual}.csv"
        df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
        print(f"📁 Archivo guardado en: {ruta_archivo}")
