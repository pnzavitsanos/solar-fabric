# SOLAR Data Platform presentation

## ▶ [Open the presentation in your browser](https://pnzavitsanos.github.io/solar-fabric/presentation/solar-presentation.html)

An interactive walkthrough of the project: the pipeline, the star schema, daily energy with a plant picker, plant efficiency, a monthly heatmap, data quality and the code behind each layer. Use the vendor filter at the top and the arrow keys to move between slides.

## ⬇ [Download it](https://github.com/pnzavitsanos/solar-fabric/raw/main/presentation/solar-presentation.html)

[`solar-presentation.html`](solar-presentation.html) is one self-contained file (about 1 MB). Double-click it and it opens in any browser, even offline.

---

# Effort metric: the SOLAR effort sphere

## ▶ [Open the effort sphere in your browser](https://pnzavitsanos.github.io/solar-fabric/presentation/effort-metric.html)

A 3D view of every attribute in the pipeline, from the landing CSVs to the gold star schema, placed by how much work it took to create.

- **d = l + Σ c · m.** l counts the attribute boxes passed from landing. Each verb colour c on the path adds m, the number of arrows of that colour arriving at the attribute.
- **r = 1/d** is only for the picture: the most effort sits at the centre. Landing (d = 0) is drawn on a big outer sphere.
- **Validating is the most effort-intensive operation:** `fact_readings.is_valid` sits at the centre with d = 21.

Click a cube to light up every path that creates it. Use ← → to walk through the 72 steps and ↑ ↓ to jump between the 18 sentences.

## ⬇ [Download it](https://github.com/pnzavitsanos/solar-fabric/raw/main/presentation/effort-metric.html)

[`effort-metric.html`](effort-metric.html) is one self-contained file (about 0.7 MB) with the 3D library built in. Double-click it and it opens in any browser, even offline.

