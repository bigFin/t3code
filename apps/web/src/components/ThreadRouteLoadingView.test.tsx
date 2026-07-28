import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ThreadRouteLoadingView } from "./ThreadRouteLoadingView";

describe("ThreadRouteLoadingView", () => {
  it("renders visible reconnect progress instead of an empty route", () => {
    const markup = renderToStaticMarkup(
      <ThreadRouteLoadingView
        title="Reconnecting to Laptop..."
        description="Waiting for Laptop to provide the latest session state."
      />,
    );

    expect(markup).toContain("Reconnecting to Laptop...");
    expect(markup).toContain("Waiting for Laptop to provide the latest session state.");
    expect(markup).toContain('role="status"');
  });
});
