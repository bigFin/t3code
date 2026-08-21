# Flux companion (beta)

Flux is an opt-in experimental animated activity companion for T3 Code. It watches the
threads available from every connected environment, including remote hosts, so
you do not need an extra process or agent installed on each host.

Flux focuses the most actionable work in this order:

1. A thread waiting for approval or input
2. A failed thread
3. A recently completed thread ready for review
4. A running or retrying thread

Select Flux to open the thread it is showing. The small status card identifies
the environment and project, and tells you how many active threads and hosts
are in view.

## Show or hide Flux

- Open **Settings → Appearance** and use **Flux companion**.
- Or open the command palette and search for **Flux**, **companion**, **pet**,
  or **cat**.

## Bring your own sprite

Flux ships without any artwork. To bring it to life, give it an 8x11 sprite
sheet (PNG, or a data URL) in **Settings → Appearance → Flux companion sprite**.
The sheet is read as a grid of square cells: the first 9 rows hold the walking,
idle, and status animations, and the last 2 rows hold one static pose per
pointer direction. Until a sprite URL is set — or while it fails to load — Flux
stays hidden.

Flux appears on desktop-sized web and desktop app windows with a mouse or
trackpad. It stays still when your system requests reduced motion, and it does
not float over compact touch layouts.
