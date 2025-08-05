import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
from datetime import datetime

async def scrape_jumbo_cremosos():
    base_url = "https://www.jumbo.com.ar/queso%20cremoso?_q=queso%20cremoso&map=ft&page={page}"
    keywords = ["cremoso", "cremon"]
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    all_productos = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Cambié aquí: solo 1 y 2 (range es excluyente, así que 3 para que llegue a 2)
        for pagina in range(1, 3):
            print(f"🔄 Visitando página {pagina}...")
            await page.goto(base_url.format(page=pagina), timeout=60000)
            await page.wait_for_timeout(3000)

            nombres = await page.query_selector_all("span.vtex-product-summary-2-x-productBrand")
            precios = await page.query_selector_all("div.vtex-price-format-gallery")

            if not nombres or not precios:
                print(f"❌ Fin o error en página {pagina}")
                break

            productos_validos = 0

            for nombre_elem, precio_elem in zip(nombres, precios):
                nombre = (await nombre_elem.inner_text()).strip()
                precio_raw = (await precio_elem.inner_text()).strip()
                precio = precio_raw.replace("$", "").replace(".", "").replace(",", ".").strip()

                if any(kw in nombre.lower() for kw in keywords):
                    all_productos.append({
                        "fecha": fecha_actual,
                        "nombre": nombre,
                        "precio": precio
                    })
                    productos_validos += 1

            if productos_validos == 0:
                print(f"⛔ Sin productos válidos en página {pagina}, se detiene el scraping.")
                break

        await browser.close()

    return all_productos

if __name__ == "__main__":
    resultados = asyncio.run(scrape_jumbo_cremosos())
    print(f"\n✅ Se encontraron {len(resultados)} productos que contienen 'cremoso' o 'cremon'.")

    if resultados:
        df = pd.DataFrame(resultados)
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        os.makedirs("Data/Raw", exist_ok=True)
        ruta_archivo = f"Data/Raw/jumbo_raw_{fecha_actual}.csv"
        df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
        print(f"📁 Archivo guardado en: {ruta_archivo}")
