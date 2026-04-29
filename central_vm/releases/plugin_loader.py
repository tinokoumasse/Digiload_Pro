"""
Digiload Pro — Plugin Loader
Phase 8: Scans /plugins/ folder, loads enabled plugins, fires events

Usage in core (digiload_pro.py):
    from plugin_loader import PluginLoader
    plugins = PluginLoader("plugins", config)
    plugins.on_start()
    plugins.on_state_change("ARMED", ctx)
    plugins.on_validated("376020400000000001", ctx)
    plugins.on_stop()

A plugin is any .py file in /plugins/ that implements one or more hooks.
The core never needs to change when a new plugin is added.
"""

import os
import importlib.util
import logging

log = logging.getLogger("digiload.plugins")


class PluginLoader:
    """
    Discovers, loads, and dispatches events to all enabled plugins.
    """

    def __init__(self, plugins_dir: str, config: dict):
        self._plugins    = []   # list of loaded plugin modules
        self._config     = config
        self._plugins_dir = plugins_dir
        self._load_all()

    # ── Loading ───────────────────────────────────────────────────────────────
    def _load_all(self):
        """Scan plugins_dir and load all enabled .py files."""
        if not os.path.isdir(self._plugins_dir):
            log.warning(f"[plugins] Directory not found: {self._plugins_dir} — no plugins loaded")
            return

        plugin_cfg = self._config.get("plugins", {})

        for filename in sorted(os.listdir(self._plugins_dir)):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue

            name = filename[:-3]   # strip .py

            # Check enabled flag in config — default True if not specified
            cfg_entry = plugin_cfg.get(name, {})
            if isinstance(cfg_entry, dict):
                enabled = cfg_entry.get("enabled", True)
            else:
                enabled = bool(cfg_entry)

            if not enabled:
                log.info(f"[plugins] {name} — disabled in config, skipping")
                continue

            self._load_plugin(name, os.path.join(self._plugins_dir, filename), cfg_entry)

        log.info(f"[plugins] {len(self._plugins)} plugin(s) loaded: "
                 f"{[p.__name__.split('.')[-1] for p in self._plugins]}")

    def _load_plugin(self, name: str, filepath: str, plugin_cfg: dict):
        """Load a single plugin file as a module."""
        try:
            spec   = importlib.util.spec_from_file_location(f"plugin_{name}", filepath)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Inject plugin-specific config if available
            if hasattr(module, "configure") and callable(module.configure):
                module.configure(plugin_cfg, self._config)

            self._plugins.append(module)
            log.info(f"[plugins] ✅ Loaded: {name}")
        except Exception as e:
            log.error(f"[plugins] ❌ Failed to load {name}: {e}")

    def reload(self, config: dict):
        """Hot-reload all plugins (call after config push from VM)."""
        log.info("[plugins] Hot-reloading plugins...")
        self._plugins.clear()
        self._config = config
        self._load_all()

    # ── Event dispatch ────────────────────────────────────────────────────────
    def _fire(self, hook: str, *args, **kwargs):
        """Call hook on all plugins that implement it. Never raises."""
        for plugin in self._plugins:
            fn = getattr(plugin, hook, None)
            if callable(fn):
                try:
                    fn(*args, **kwargs)
                except Exception as e:
                    name = getattr(plugin, "__name__", "unknown")
                    log.error(f"[plugins] {name}.{hook} error: {e}")

    def on_start(self, config: dict):
        """Called once after core initialisation."""
        self._fire("on_start", config)

    def on_state_change(self, mode: str, ctx: dict):
        """
        Called on every state machine transition.
        mode: 'IDLE' | 'STANDBY' | 'ARMED' | 'VALIDATED' |
              'ERROR_SSCC' | 'ERROR_FORKLIFT'
        ctx:  { gate_id, gate_name, sscc, tour_id, forklift_id }
        """
        self._fire("on_state_change", mode, ctx)

    def on_frame(self, frame):
        """
        Called on every camera frame (keep implementations FAST).
        frame: numpy BGR array
        """
        self._fire("on_frame", frame)

    def on_validated(self, sscc: str, ctx: dict):
        """
        Called when a pallet is confirmed loaded.
        ctx: { gate_id, gate_name, tour_id, forklift_id, loaded, total }
        """
        self._fire("on_validated", sscc, ctx)

    def on_error(self, error_type: str, ctx: dict):
        """
        Called on ERROR_SSCC or ERROR_FORKLIFT.
        error_type: 'WRONG_SSCC' | 'WRONG_FORKLIFT'
        ctx: { gate_id, sscc, forklift_detected }
        """
        self._fire("on_error", error_type, ctx)

    def on_stop(self):
        """Called on clean shutdown."""
        self._fire("on_stop")

    # ── Info ──────────────────────────────────────────────────────────────────
    def loaded_names(self) -> list:
        return [getattr(p, "__name__", "?").replace("plugin_", "") for p in self._plugins]

    def __len__(self):
        return len(self._plugins)
