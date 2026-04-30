// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const siteUrl = 'https://gatherstep.dev';
const siteTitle = 'Gather Step';
const siteDescription =
  'Local-first code graph for AI coding assistants. Trace routes, events, contracts, and change impact across every repo before you edit.';
const socialImage = `${siteUrl}/og_card.webp`;

// https://astro.build/config
export default defineConfig({
  site: siteUrl,
  integrations: [
    starlight({
      title: siteTitle,
      description: siteDescription,
      favicon: '/favicon/favicon.ico',
      head: [
        {
          tag: 'meta',
          attrs: {
            name: 'theme-color',
            content: '#101418',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'robots',
            content: 'index,follow',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'icon',
            type: 'image/svg+xml',
            href: '/favicon/favicon.svg',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'icon',
            type: 'image/png',
            sizes: '96x96',
            href: '/favicon/favicon-96x96.png',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'apple-touch-icon',
            sizes: '180x180',
            href: '/favicon/apple-touch-icon.png',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'manifest',
            href: '/favicon/site.webmanifest',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image',
            content: socialImage,
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image:alt',
            content: `${siteTitle} social preview card`,
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:image',
            content: socialImage,
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:image:alt',
            content: `${siteTitle} social preview card`,
          },
        },
      ],
      // logo: { src: './src/assets/logo.svg', replacesTitle: true },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/thedoublejay/gather-step',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/thedoublejay/gather-step/edit/main/website/',
      },
      disable404Route: true,
      components: {
        Header: './src/components/starlight/Header.astro',
        Footer: './src/components/starlight/Footer.astro',
        MobileMenuFooter: './src/components/starlight/MobileMenuFooter.astro',
        MarkdownContent: './src/components/starlight/MarkdownContent.astro',
        PageTitle: './src/components/starlight/PageTitle.astro',
        ThemeSelect: './src/components/starlight/ThemeSelect.astro',
      },
      lastUpdated: true,
      pagination: true,
      sidebar: [
        {
          label: 'Overview',
          items: [
            { slug: 'guides/getting-started' },
            { slug: 'about' },
          ],
        },
        {
          label: 'Setup',
          items: [
            { slug: 'guides/installation' },
            { slug: 'guides/workspace-setup' },
            { slug: 'guides/mcp-clients' },
          ],
        },
        {
          label: 'Concepts',
          items: [
            { slug: 'concepts/polyrepo-graph' },
            { slug: 'concepts/event-topology' },
            { slug: 'concepts/context-packs' },
            { slug: 'concepts/deterministic-indexing' },
            { slug: 'concepts/language-support' },
            { slug: 'concepts/architecture' },
          ],
        },
        {
          label: 'Workflows',
          items: [
            { slug: 'guides/operator-workflows' },
            { slug: 'guides/memory-backed-planning' },
            { slug: 'guides/data-shape-verification' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { slug: 'reference/cli' },
            { slug: 'reference/configuration' },
            { slug: 'reference/mcp-tools' },
            { slug: 'changelog' },
          ],
        },
      ],
      // Design tokens land here once the external design is translated to CSS.
      customCss: ['./src/styles/custom.css'],
    }),
  ],
});
