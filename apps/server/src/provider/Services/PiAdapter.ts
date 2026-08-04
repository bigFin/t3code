/**
 * PiAdapter — shape type for the Pi Agent provider adapter.
 *
 * The driver model bundles one adapter per configured Pi Agent instance, so
 * this module is only a naming anchor for the per-instance adapter contract.
 *
 * @module PiAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface PiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
