import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
from datetime import datetime

async def scrape_anonima_cremosos():
    base_url = "https://supermercado.laanonimaonline.com/buscar?pag={page}&clave=queso+cremoso"
    keywords = ["cremoso", "cremon"]
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    all_productos = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="es-AR",
        )
        page = await context.new_page()

        # Inyectar script para camuflar headless
        await page.add_init_script(
            """() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['es-AR', 'es'] });
            }"""
        )

        for pagina in range(1, 3):  # primeras 2 páginas
            print(f"🔄 Visitando página {pagina}...")
            await page.goto(base_url.format(page=pagina), timeout=60000)
            # Esperar que carguen los productos - 5 segundos
            await page.wait_for_timeout(5000)

            productos = await page.query_selector_all("div.producto")

            if not productos:
                print(f"❌ No se encontraron productos en página {pagina}")
                break

            productos_validos = 0

            for producto in productos:
                clases = await producto.get_attribute("class") or ""
                if "sin_stock" in clases:
                    continue
                if not await producto.is_visible():
                    continue

                nombre_elem = await producto.query_selector("a[id^='btn_nombre_imetrics_']")
                precio_elem = await producto.query_selector("div.precio.semibold.aux1")

                if not nombre_elem or not precio_elem:
                    continue

                nombre = (await nombre_elem.inner_text()).strip()
                precio_entero = (await precio_elem.inner_text()).strip()  # Ej: "$ 14.900"
                precio_str = precio_entero.replace("$", "").replace(".", "").replace(",00", "").strip()

                if any(kw in nombre.lower() for kw in keywords):
                    all_productos.append({
                        "fecha": fecha_actual,
                        "nombre": nombre,
                        "precio": precio_str
                    })
                    productos_validos += 1

            if productos_validos == 0:
                print(f"⛔ Sin productos válidos con stock visibles en página {pagina}, se detiene el scraping.")
                break

        await browser.close()

    return all_productos

if __name__ == "__main__":
    resultados = asyncio.run(scrape_anonima_cremosos())
    print(f"\n✅ Se encontraron {len(resultados)} productos que contienen 'cremoso' o 'cremon' y están en stock visibles.")

    if resultados:
        df = pd.DataFrame(resultados).drop_duplicates()
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        os.makedirs("Data/Raw", exist_ok=True)
        ruta_archivo = f"Data/Raw/anonima_raw_{fecha_actual}.csv"
        df.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
        print(f"📁 Archivo guardado en: {ruta_archivo}")
