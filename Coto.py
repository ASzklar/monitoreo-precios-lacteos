import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
from datetime import datetime
import re

async def scrape_coto_cremosos():
    url = (
        "https://www.cotodigital.com.ar/sitios/cdigi/categoria/catalogo-frescos-quesos-quesos-blandos/_/N-1ekbxyw"
        "?Dy=1&Nf=product.startDate%7CLTEQ%201.75392E12%7C%7Cproduct.endDate%7CGTEQ%201.75392E12"
        "&Nr=AND(product.sDisp_200:1004,product.language:espa%C3%B1ol,OR(product.siteId:CotoDigital))"
        "&Ntt=queso%20cremoso&idSucursal=200"
    )

    keywords = ["cremoso", "cremon"]
    fecha_actual = datetime.now().strftime('%Y-%m-%d')

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
        ])
        context = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ))
        page = await context.new_page()
        await page.goto(url)
        await asyncio.sleep(5)

        all_productos = []

        while True:
            try:
                await page.wait_for_selector("div.centro-precios", timeout=40000)
            except:
                break

            productos = await page.query_selector_all("div.centro-precios")
            for producto in productos:
                nombre_elem = await producto.query_selector("h3.nombre-producto")
                precio_elem = await producto.query_selector("h4.card-title")
                if nombre_elem and precio_elem:
                    nombre = (await nombre_elem.inner_text()).strip()
                    precio_raw = (await precio_elem.inner_text()).strip()

                    if any(kw in nombre.lower() for kw in keywords):
                        precio_numero = precio_raw.replace(".", "").replace(",00", "").replace("$", "").strip()
                        all_productos.append({
                            "fecha": fecha_actual,
                            "nombre": nombre,
                            "precio": precio_numero
                        })

            siguiente = await page.query_selector("a.page-link.page-back-next:has-text('Siguiente')")
            if siguiente and await siguiente.is_visible():
                clases = await siguiente.get_attribute("class")
                if clases and "disabled" in clases:
                    break
                await siguiente.click()
                await asyncio.sleep(5)
            else:
                break

        await browser.close()

    return all_productos

if __name__ == "__main__":
    resultados = asyncio.run(scrape_coto_cremosos())
    print(f"Se encontraron {len(resultados)} productos que contienen 'cremoso' o 'cremon'.")

    # Guardar en CSV
    if resultados:
        df = pd.DataFrame(resultados)
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        os.makedirs("Data/Raw", exist_ok=True)
        ruta_archivo = f"Data/Raw/coto_raw_{fecha_actual}.csv"
        df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
        print(f"Archivo guardado en: {ruta_archivo}")
