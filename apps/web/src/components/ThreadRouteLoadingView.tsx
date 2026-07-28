import type { ThreadRouteLoadingCopy } from "../threadRoutes";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "./ui/empty";
import { SidebarInset } from "./ui/sidebar";
import { Spinner } from "./ui/spinner";

export function ThreadRouteLoadingView({ title, description }: ThreadRouteLoadingCopy) {
  return (
    <SidebarInset className="h-svh min-h-0 overflow-hidden overscroll-y-none bg-background text-foreground md:h-dvh">
      <Empty>
        <Spinner className="size-5 text-muted-foreground" />
        <EmptyHeader>
          <EmptyTitle>{title}</EmptyTitle>
          <EmptyDescription>{description}</EmptyDescription>
        </EmptyHeader>
      </Empty>
    </SidebarInset>
  );
}
