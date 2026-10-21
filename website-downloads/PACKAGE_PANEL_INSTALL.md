# Install the integrated panels

Copy the contents of `admin_clean.html` into your existing cPanel admin-panel file, and `trader_clean.html` into your existing trader-panel file. Keep your existing cPanel filenames and URLs. Both files include their package JavaScript and styles; no separate JavaScript file or ZIP is required.

Admin: **Business settings → Package prices & benefits**. Each benefit's value, charge type, price and active status is editable. Existing products and service switches remain in collapsible sections. Existing purchase terms are preserved.

Trader: **Business → Build Your Package**. This uses the preferred black-and-gold cards, benefit selectors, price summary and mobile price bar. Traders can experiment and save a non-financial draft on the same browser. Saved backend prices determine the total. Before prices are configured, the display says the price is coming soon and does not invent an offer.

Before launch, the purchase button says **Purchases open at launch** and is disabled. The backend independently rejects new order quotes and submissions. Previewing or saving a draft creates no payment, purchase, broker account or payout. Existing purchase history remains available during a sales pause.

Package sales remain closed until the owner launches the product and engine readiness checks pass. Copying these HTML files does not deploy Windows VPS workers or verify engine integration.

Current standard and premium plans can be edited directly. Plan changes apply to new purchases. The backend preserves historical purchase terms automatically; copying a product is optional.
