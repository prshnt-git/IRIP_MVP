import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

type BadgeVariant = "default" | "secondary" | "destructive" | "outline";

const variantClasses: Record<BadgeVariant, string> = {
  default: "bg-zinc-900 text-zinc-50 hover:bg-zinc-900/80",
  secondary: "bg-zinc-100 text-zinc-900 hover:bg-zinc-100/80",
  destructive: "bg-red-500 text-white hover:bg-red-500/80",
  outline: "border border-zinc-200 text-zinc-950 bg-transparent",
};

interface BadgeProps extends HTMLAttributes<HTMLDivElement> {
  variant?: BadgeVariant;
}

function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return (
    <div
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold transition-colors",
        variantClasses[variant],
        className
      )}
      {...props}
    />
  );
}

export { Badge };
export type { BadgeProps, BadgeVariant };
